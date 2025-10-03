#!/usr/bin/env python3
"""
MySQL OLTP Setup & Transactions

Implements the minimal e-commerce schema and generates realistic transactional data.
- init: create tables
- seed: insert customers & products
- orders: create orders & order_items with consistent currency math
- stream: continuously create orders
- live: ensure schema + seed, stream orders, periodically reprice products

By default prints SQL. Use --apply with a DSN to execute against MySQL.
DSN format: "host=localhost user=root password=password database=shop port=3306"
Requires: mysql-connector-python (only when using --apply)
"""

import argparse
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from constants import (
    CHANNEL_PRIORS,
    CURRENCY,
    CUSTOMER_COUNT,
    MARKETING_CAMPAIGNS as CAMPAIGNS,
    PRODUCT_COUNT,
    SCHEMA_SQL,
)

from utils import DEFAULT_SEED, generate_customers, generate_products, parse_date, weighted_choice

random.seed(DEFAULT_SEED)

STATUS_TRANSITIONS = {
    "created": ("paid",),
    "paid": ("shipped", "shipped","shipped", "shipped", "shipped", "shipped", "cancelled"),  # repeat shipped to bias toward fulfillment
    "cancelled": ("refunded",),
}
TERMINAL_STATUSES: Tuple[str, ...] = tuple(sorted({"shipped", "refunded"}))
ADVANCE_EARLIEST_CREATED_SQL = "now() - interval 10 hour"
ADVANCE_LATEST_CREATED_SQL = "now() - interval 5 minute"

# ---------- Optional MySQL connector ----------

def _maybe_get_conn(dsn: str):
    try:
        import mysql.connector  # type: ignore
    except Exception:
        raise RuntimeError("mysql-connector-python is required for --apply. Install it or omit --apply to print SQL.")
    # Parse simple "key=value" tokens
    kwargs: Dict[str, str] = {}
    for token in dsn.split():
        if "=" in token:
            k, v = token.split("=", 1)
            kwargs[k.strip()] = v.strip()
    # Provide sensible defaults
    kwargs.setdefault("port", "3306")
    conn = mysql.connector.connect(
        host=kwargs.get("host", "localhost"),
        user=kwargs.get("user"),
        password=kwargs.get("password"),
        database=kwargs.get("database"),
        port=int(kwargs.get("port", "3306")),
        autocommit=False,
    )
    return conn

# ---------- Helpers ----------
def uuid4():
    import uuid
    return str(uuid.uuid4())

def now_utc():
    return datetime.now(timezone.utc)

def iso_ts(dt: datetime):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def pick_campaign(channel: str) -> Optional[str]:
    return random.choice(CAMPAIGNS[channel])

def sample_customer_id(pop: Sequence[str]) -> str:
    return random.choice(pop)

def sample_items(product_ids_prices: Dict[str, float]) -> List[Tuple[str,int,float,float]]:
    """Return list of (product_id, qty, unit_price, line_amount)."""
    num_lines = 1 + int(random.random() < 0.35) + int(random.random() < 0.15)  # 1-3 lines, skewed to 1
    chosen = random.sample(list(product_ids_prices.keys()), k=min(num_lines, len(product_ids_prices)))
    items = []
    for pid in chosen:
        qty = 1 + (1 if random.random() < 0.2 else 0)  # mostly 1, sometimes 2
        unit_price = product_ids_prices[pid]
        line_amount = round(qty * unit_price, 2)
        items.append((pid, qty, unit_price, line_amount))
    return items

def tax_rate_for_country(country_code: str) -> float:
    # Simplified: 19% default (DE), slightly varied range for demo realism
    base = {
        "DE": 0.19, "FR": 0.20, "ES": 0.21, "IT": 0.22, "NL": 0.21,
        "BE": 0.21, "AT": 0.20, "SE": 0.25, "DK": 0.25, "PL": 0.23
    }.get(country_code, 0.20)
    return base

def shipping_fee_for_subtotal(subtotal: float) -> float:
    if subtotal >= 100:
        return 0.00
    if subtotal >= 60:
        return 2.99
    return 4.99

def status_from_total(_total: float) -> str:
    return "created"


def advance_open_orders(cur, max_updates: int) -> int:
    if max_updates <= 0:
        return 0

    time_window_clause = (
        f"created_at < {ADVANCE_LATEST_CREATED_SQL}"
    )
    if TERMINAL_STATUSES:
        placeholders = ",".join(["%s"] * len(TERMINAL_STATUSES))
        query = (
            "select order_id, status, created_at < (now() - interval 5 hour) as is_earliest from orders "
            f"where status not in ({placeholders}) and {time_window_clause} "
            "order by is_earliest desc, rand() limit %s"
        )
        params = (*TERMINAL_STATUSES, max_updates)
    else:
        query = (
            "select order_id, status from orders "
            f"where {time_window_clause} order by rand() limit %s"
        )
        params = (max_updates,)
    cur.execute(query, params)
    rows = cur.fetchall()
    advanced = 0
    for order_id, status, _ in rows:
        transitions = STATUS_TRANSITIONS.get(str(status))
        if not transitions:
            continue
        next_state = random.choice(transitions)
        cur.execute(
            "update orders set status=%s, updated_at=now() where order_id=%s",
            (next_state, order_id),
        )
        advanced += 1
    return advanced

# ---------- SQL emit / apply helpers ----------

def exec_many(conn, statements: List[str]) -> None:
    cur = conn.cursor()
    try:
        for st in statements:
            cur.execute(st)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

def out_or_apply(statements: List[str], apply: bool, dsn: Optional[str], outfile: Optional[Path]) -> None:
    if apply:
        if not dsn:
            raise RuntimeError("--apply requires --dsn")
        conn = _maybe_get_conn(dsn)
        try:
            exec_many(conn, statements)
        finally:
            conn.close()
        print(f"Applied {len(statements)} statements.")
    else:
        sql_blob = ";\n".join(s.rstrip(";") for s in statements) + ";\n"
        if outfile:
            outfile.write_text(sql_blob, encoding="utf-8")
            print(f"Wrote SQL to {outfile}")
        else:
            sys.stdout.write(sql_blob)


def ensure_schema_and_seed(dsn: str) -> None:
    """Create tables if missing and seed base data when tables are empty."""
    # Always run init; the statements use IF NOT EXISTS so this is idempotent.
    cmd_init(apply=True, dsn=dsn, outfile=None)

    conn = _maybe_get_conn(dsn)
    try:
        cur = conn.cursor()
        cur.execute("select count(*) from customers")
        customer_count = cur.fetchone()[0]
        cur.execute("select count(*) from products")
        product_count = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()

    if customer_count == 0 or product_count == 0:
        cmd_seed(apply=True, dsn=dsn, outfile=None)

# ---------- Commands ----------

def cmd_reprice(apply: bool, dsn: Optional[str], outfile: Optional[Path],
                pct_min: float, pct_max: float, direction: str,
                count: Optional[int], per_day: Optional[int],
                backfill_start: Optional[str], backfill_end: Optional[str],
                only_active: bool, scd2_log_csv: Optional[Path]) -> None:
    """Generate product price changes to simulate SCD2.
    Either perform a one-shot set of updates (no backfill_* given), or
    spread updates uniformly between backfill_start..backfill_end with up to per_day updates.
    Writes UPDATE statements (or applies), and optionally a CSV change log.
    """
    import csv

    # Helper: choose sign based on direction
    def sample_delta() -> float:
        import random
        pct = random.uniform(pct_min, pct_max) / 100.0
        if direction == 'up':
            return pct
        elif direction == 'down':
            return -pct
        else:
            return pct if random.random() < 0.5 else -pct

    # Load product list & current prices
    products: List[Tuple[str, float, bool]] = []  # (product_id, price, is_active)
    if apply:
        if not dsn:
            raise RuntimeError("--apply requires --dsn")
        conn = _maybe_get_conn(dsn)
        cur = conn.cursor()
        cur.execute("select product_id, price, is_active from products")
        rows = cur.fetchall()
        for pid, price, active in rows:
            products.append((str(pid), float(price), bool(active)))
        cur.close()
        conn.close()
    else:
        # Synthesize a small catalog if not applying (for SQL preview)
        for product in generate_products(PRODUCT_COUNT):
            products.append((product["product_id"], product["price"], True))

    if only_active:
        products = [p for p in products if p[2]]

    if not products:
        raise RuntimeError("No products available to reprice.")

    # Build update plan (timestamp, product_id, old_price, new_price)
    plan: List[Tuple[str, str, float, float]] = []
    rng = random.Random(DEFAULT_SEED)

    if backfill_start and backfill_end:
        start = parse_date(backfill_start)
        end = parse_date(backfill_end)
        if end < start:
            raise RuntimeError("backfill_end must be >= backfill_start")
        days = (end - start).days + 1
        per_day = per_day or 5
        picks = products[:]
        rng.shuffle(picks)
        idx = 0
        for d in range(days):
            day = start + timedelta(days=d)
            n = per_day if count is None else min(per_day, max(0, count - len(plan)))
            for _ in range(n):
                if idx >= len(picks):
                    idx = 0
                    rng.shuffle(picks)
                pid, old_price, _active = picks[idx]
                idx += 1
                delta = sample_delta()
                new_price = max(0.01, round(old_price * (1.0 + delta), 2))
                ts = day.replace(hour=rng.randint(8,18), minute=rng.randint(0,59), second=rng.randint(0,59))
                plan.append((ts.strftime("%Y-%m-%d %H:%M:%S"), pid, old_price, new_price))
            if count is not None and len(plan) >= count:
                break
    else:
        # One-shot updates with current timestamp
        now = now_utc().strftime("%Y-%m-%d %H:%M:%S")
        n = count or max(1, len(products)//3)
        picks = rng.sample(products, k=min(n, len(products)))
        for pid, old_price, _active in picks:
            delta = sample_delta()
            new_price = max(0.01, round(old_price * (1.0 + delta), 2))
            plan.append((now, pid, old_price, new_price))

    # Emit SQL or apply
    statements: List[str] = []
    for ts, pid, old_price, new_price in plan:
        statements.append(
            f"update products set price={new_price:.2f}, updated_at='{ts}' where product_id='{pid}'"
            + (" and is_active = TRUE" if only_active else "")
        )

    # Optional SCD2 change log CSV
    if scd2_log_csv:
        with scd2_log_csv.open('w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(["updated_at","product_id","old_price","new_price","currency"]) 
            for ts, pid, old_p, new_p in plan:
                w.writerow([ts, pid, f"{old_p:.2f}", f"{new_p:.2f}", "EUR"]) 

    out_or_apply(statements, apply, dsn, outfile)


def cmd_init(apply: bool, dsn: Optional[str], outfile: Optional[Path]) -> None:
    statements = [x.strip() for x in SCHEMA_SQL.strip().split(";\n\n") if x.strip()]
    out_or_apply(statements, apply, dsn, outfile)

def cmd_seed(apply: bool, dsn: Optional[str], outfile: Optional[Path]) -> None:
    statements: List[str] = []

    # Customers
    customer_records = generate_customers(CUSTOMER_COUNT, reference_time=now_utc())
    for record in customer_records:
        opt_in = "TRUE" if record["marketing_opt_in"] else "FALSE"
        created = iso_ts(record["created_at"])
        statements.append(
            "insert into customers (customer_id,email,full_name,country_code,marketing_opt_in,created_at) "
            f"values ('{record['customer_id']}','{record['email']}','{record['full_name']}',"
            f"'{record['country_code']}',{opt_in},'{created}')"
        )

    # Products
    product_records = generate_products(PRODUCT_COUNT)
    updated_at = iso_ts(now_utc())
    for product in product_records:
        statements.append(
            f"insert into products (product_id,name,category,price,currency,is_active,updated_at) "
            f"values ('{product['product_id']}','{product['name']}','{product['category']}',"
            f"{product['price']:.2f},'{CURRENCY}',TRUE,'{updated_at}')"
        )

    out_or_apply(statements, apply, dsn, outfile)

def cmd_orders(count: int, apply: bool, dsn: Optional[str], outfile: Optional[Path],
               min_lines: int, max_lines: int, backfill_days: int) -> None:
    # For orders, we need product prices and customer list; if applying, fetch from DB; else, synthesize matching IDs
    product_ids_prices: Dict[str,float] = {}
    customer_ids: List[str] = []
    if apply:
        if not dsn:
            raise RuntimeError("--apply requires --dsn")
        conn = _maybe_get_conn(dsn)
        cur = conn.cursor()
        cur.execute("select product_id, price from products where is_active = TRUE")
        for pid, price in cur.fetchall():
            product_ids_prices[str(pid)] = float(price)
        cur.execute("select customer_id, country_code from customers")
        customers_rows = cur.fetchall()
        customer_ids = [str(r[0]) for r in customers_rows]
        cust_country = {str(r[0]): str(r[1]) for r in customers_rows}
        cur.close()
        conn.close()
    else:
        # Synthesize IDs (align with seed defaults)
        synthesized_customers = generate_customers(CUSTOMER_COUNT)
        customer_ids = [c["customer_id"] for c in synthesized_customers]
        # simple default country for tax calc when printing SQL
        cust_country = {c["customer_id"]: c["country_code"] for c in synthesized_customers}
        for product in generate_products(PRODUCT_COUNT):
            product_ids_prices[product["product_id"]] = product["price"]

    if not product_ids_prices or not customer_ids:
        raise RuntimeError("No products or customers found. Run 'seed' first (or provide --apply with existing data).")

    statements: List[str] = []

    # backfill order_ts over the last N days uniformly
    start_time = now_utc() - timedelta(days=backfill_days)
    for _ in range(count):
        order_id = uuid4()
        cid = random.choice(customer_ids)
        country = cust_country[cid]
        order_ts = start_time + timedelta(seconds=random.randint(0, backfill_days * 24 * 3600))
        channel = weighted_choice(CHANNEL_PRIORS)
        campaign_id = pick_campaign(channel)
        items = sample_items(product_ids_prices)
        # Restrict number of items if min/max provided
        if max_lines is not None:
            items = items[:max_lines]
        if min_lines is not None and len(items) < min_lines:
            # pad by adding more unique products if possible
            extra_needed = min_lines - len(items)
            remaining = [p for p in product_ids_prices.keys() if p not in [i[0] for i in items]]
            for pid in remaining[:extra_needed]:
                qty = 1
                unit_price = product_ids_prices[pid]
                line_amount = round(qty * unit_price, 2)
                items.append((pid, qty, unit_price, line_amount))

        subtotal = round(sum(li[3] for li in items), 2)
        tax = round(subtotal * tax_rate_for_country(country), 2)
        shipping_fee = round(shipping_fee_for_subtotal(subtotal), 2)
        total_amount = round(subtotal + tax + shipping_fee, 2)
        status = status_from_total(total_amount)

        campaign_value = "NULL" if campaign_id is None else f"'{campaign_id}'"
        created_at = iso_ts(now_utc())
        order_columns = (
            "order_id,customer_id,order_ts,status,currency,"
            "subtotal,tax,shipping_fee,total_amount,channel,campaign_id,created_at,updated_at"
        )
        order_values = (
            f"'{order_id}','{cid}','{iso_ts(order_ts)}','{status}','{CURRENCY}',"
            f"{subtotal:.2f},{tax:.2f},{shipping_fee:.2f},{total_amount:.2f},"
            f"'{channel}',{campaign_value},'{created_at}','{created_at}'"
        )
        statements.append(
            f"insert into orders ({order_columns}) values ({order_values})"
        )
        for pid, qty, unit_price, line_amount in items:
            statements.append(
                "insert into order_items (order_id,product_id,qty,unit_price,line_amount) "
                f"values ('{order_id}','{pid}',{qty},{unit_price:.2f},{line_amount:.2f})"
            )

    out_or_apply(statements, apply, dsn, outfile)

def cmd_stream(eps: float, apply: bool, dsn: Optional[str]) -> None:
    if not apply:
        raise RuntimeError("stream requires --apply to insert into a live database.")
    if not dsn:
        raise RuntimeError("--apply requires --dsn")

    conn = _maybe_get_conn(dsn)
    cur = conn.cursor()

    # preload products & customers
    cur.execute("select product_id, price from products where is_active = TRUE")
    products = cur.fetchall()
    if not products:
        raise RuntimeError("No products in DB. Run seed first.")
    product_ids_prices = {str(pid): float(price) for pid, price in products}

    cur.execute("select customer_id, country_code from customers")
    customers_rows = cur.fetchall()
    if not customers_rows:
        raise RuntimeError("No customers in DB. Run seed first.")
    customer_ids = [str(r[0]) for r in customers_rows]
    cust_country = {str(r[0]): str(r[1]) for r in customers_rows}

    try:
        while True:
            # burst one order per tick based on eps
            if random.random() < eps:  # approx eps ~ orders/second
                order_id = uuid4()
                cid = random.choice(customer_ids)
                country = cust_country[cid]
                order_ts = datetime.utcnow()
                channel = weighted_choice(CHANNEL_PRIORS)
                campaign_id = pick_campaign(channel)
                items = sample_items(product_ids_prices)
                subtotal = round(sum(li[3] for li in items), 2)
                tax = round(subtotal * tax_rate_for_country(country), 2)
                shipping_fee = round(shipping_fee_for_subtotal(subtotal), 2)
                total_amount = round(subtotal + tax + shipping_fee, 2)
                status = status_from_total(total_amount)
                created_ts = datetime.utcnow()

                cur.execute(
                    "insert into orders (order_id,customer_id,order_ts,status,currency,subtotal,tax,shipping_fee,total_amount,channel,campaign_id,created_at,updated_at) "
                    "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        order_id,
                        cid,
                        order_ts,
                        status,
                        CURRENCY,
                        subtotal,
                        tax,
                        shipping_fee,
                        total_amount,
                        channel,
                        campaign_id,
                        created_ts,
                        created_ts,
                    )
                )
                for pid, qty, unit_price, line_amount in items:
                    cur.execute(
                        "insert into order_items (order_id,product_id,qty,unit_price,line_amount) values (%s,%s,%s,%s,%s)",
                        (order_id, pid, qty, unit_price, line_amount)
                    )
                conn.commit()
                print(f"Inserted order {order_id} ({len(items)} lines) total={total_amount:.2f} {CURRENCY}")

            progressed = advance_open_orders(cur, random.randint(0, 3))
            if progressed:
                conn.commit()
                print(f"Advanced {progressed} open order(s) to the next status")
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Stopping stream...")
    finally:
        cur.close()
        conn.close()


def cmd_live(
    eps: float,
    reprice_chance: float,
    reprice_cooldown: float,
    reprice_count: int,
    reprice_pct_min: float,
    reprice_pct_max: float,
    reprice_direction: str,
    reprice_only_active: bool,
    apply: bool,
    dsn: Optional[str],
) -> None:
    if not apply:
        raise RuntimeError("live requires --apply to work against MySQL.")
    if not dsn:
        raise RuntimeError("--apply requires --dsn")

    ensure_schema_and_seed(dsn)

    conn = _maybe_get_conn(dsn)
    cur = conn.cursor()

    def refresh_catalog() -> Tuple[Dict[str, float], Dict[str, str], List[str]]:
        cur.execute("select product_id, price from products where is_active = TRUE")
        product_rows = cur.fetchall()
        if not product_rows:
            raise RuntimeError("No active products available for streaming.")
        products_map = {str(pid): float(price) for pid, price in product_rows}

        cur.execute("select customer_id, country_code from customers")
        customer_rows = cur.fetchall()
        if not customer_rows:
            raise RuntimeError("No customers available for streaming.")
        customer_ids_local = [str(row[0]) for row in customer_rows]
        country_map = {str(row[0]): str(row[1]) for row in customer_rows}
        return products_map, country_map, customer_ids_local

    product_ids_prices, cust_country, customer_ids = refresh_catalog()
    last_reprice = time.monotonic() - reprice_cooldown  # allow immediate reprice if triggered

    try:
        while True:
            # Optionally insert an order for this tick.
            if random.random() < eps:
                order_id = uuid4()
                cid = random.choice(customer_ids)
                country = cust_country[cid]
                order_ts = datetime.utcnow()
                channel = weighted_choice(CHANNEL_PRIORS)
                campaign_id = pick_campaign(channel)
                items = sample_items(product_ids_prices)
                subtotal = round(sum(li[3] for li in items), 2)
                tax = round(subtotal * tax_rate_for_country(country), 2)
                shipping_fee = round(shipping_fee_for_subtotal(subtotal), 2)
                total_amount = round(subtotal + tax + shipping_fee, 2)
                status = status_from_total(total_amount)
                created_ts = datetime.utcnow()

                cur.execute(
                    "insert into orders (order_id,customer_id,order_ts,status,currency,subtotal,tax,shipping_fee,total_amount,channel,campaign_id,created_at,updated_at) "
                    "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        order_id,
                        cid,
                        order_ts,
                        status,
                        CURRENCY,
                        subtotal,
                        tax,
                        shipping_fee,
                        total_amount,
                        channel,
                        campaign_id,
                        created_ts,
                        created_ts,
                    ),
                )
                for pid, qty, unit_price, line_amount in items:
                    cur.execute(
                        "insert into order_items (order_id,product_id,qty,unit_price,line_amount) values (%s,%s,%s,%s,%s)",
                        (order_id, pid, qty, unit_price, line_amount),
                    )
                conn.commit()
                print(f"Inserted order {order_id} ({len(items)} lines) total={total_amount:.2f} {CURRENCY}")

            progressed = advance_open_orders(cur, random.randint(0, 3))
            if progressed:
                conn.commit()
                print(f"Advanced {progressed} open order(s) to the next status")

            now = time.monotonic()
            if (
                reprice_count > 0
                and now - last_reprice >= reprice_cooldown
                and random.random() < reprice_chance
            ):
                cmd_reprice(
                    apply=True,
                    dsn=dsn,
                    outfile=None,
                    pct_min=reprice_pct_min,
                    pct_max=reprice_pct_max,
                    direction=reprice_direction,
                    count=reprice_count,
                    per_day=None,
                    backfill_start=None,
                    backfill_end=None,
                    only_active=reprice_only_active,
                    scd2_log_csv=None,
                )
                product_ids_prices, cust_country, customer_ids = refresh_catalog()
                last_reprice = now

            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Stopping live mode...")
    finally:
        cur.close()
        conn.close()

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="OLTP (MySQL) setup and transaction generator")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # reprice
    p_reprice = sub.add_parser("reprice", help="Generate product price changes (SCD2 simulation)")
    p_reprice.add_argument("--pct-min", type=float, default=3.0, help="Min % change (absolute, default 3)")
    p_reprice.add_argument("--pct-max", type=float, default=15.0, help="Max % change (absolute, default 15)")
    p_reprice.add_argument("--direction", choices=["up","down","mixed"], default="mixed", help="Price move direction")
    p_reprice.add_argument("--count", type=int, help="Total number of updates to generate (default: ~1/3 of catalog if not backfilling)")
    p_reprice.add_argument("--per-day", type=int, help="When backfilling, max updates per day (default 5)")
    p_reprice.add_argument("--backfill-start", type=str, help="YYYY-MM-DD start date to spread updates")
    p_reprice.add_argument("--backfill-end", type=str, help="YYYY-MM-DD end date (inclusive)")
    p_reprice.add_argument("--only-active", action="store_true", help="Only update is_active products")
    p_reprice.add_argument("--scd2-log-csv", type=Path, help="Also write a CSV log of changes for warehouse tests")

    # Shared flags
    ap.add_argument("--apply", action="store_true", help="Execute statements against MySQL (requires --dsn)")
    ap.add_argument("--dsn", type=str, help='DSN like "host=localhost user=root password=secret database=shop port=3306"')
    ap.add_argument("--outfile", type=Path, help="Write SQL to this file instead of stdout")

    # init
    p_init = sub.add_parser("init", help="Create tables")
    # seed
    p_seed = sub.add_parser("seed", help="Insert customers & products")
    # orders
    p_orders = sub.add_parser("orders", help="Generate orders & order_items")
    p_orders.add_argument("--count", type=int, default=200, help="Number of orders to create")
    p_orders.add_argument("--min-lines", type=int, default=1, help="Minimum lines per order")
    p_orders.add_argument("--max-lines", type=int, default=3, help="Maximum lines per order")
    p_orders.add_argument("--backfill-days", type=int, default=14, help="Spread order_ts across last N days")
    # stream
    p_stream = sub.add_parser("stream", help="Continuously insert orders")
    p_stream.add_argument("--eps", type=float, default=0.2, help="Approx orders per second (0..1 reasonable)")
    # live
    p_live = sub.add_parser("live", help="Ensure schema/seed, stream orders, randomly reprice")
    p_live.add_argument("--eps", type=float, default=0.2, help="Approx orders per second (0..1 reasonable)")
    p_live.add_argument("--reprice-chance", type=float, default=0.05, help="Chance each second to trigger repricing")
    p_live.add_argument("--reprice-cooldown", type=float, default=120.0, help="Minimum seconds between repricing runs")
    p_live.add_argument("--reprice-count", type=int, default=3, help="Number of products to reprice when triggered")
    p_live.add_argument("--reprice-pct-min", type=float, default=3.0, help="Min %% change for repricing")
    p_live.add_argument("--reprice-pct-max", type=float, default=15.0, help="Max %% change for repricing")
    p_live.add_argument("--reprice-direction", choices=["up","down","mixed"], default="mixed", help="Direction for repricing deltas")
    p_live.add_argument("--reprice-only-active", action="store_true", help="Limit repricing to active products")

    args = ap.parse_args()

    if args.cmd == "init":
        cmd_init(args.apply, args.dsn, args.outfile)
    elif args.cmd == "seed":
        cmd_seed(args.apply, args.dsn, args.outfile)
    elif args.cmd == "orders":
        cmd_orders(args.count, args.apply, args.dsn, args.outfile, args.min_lines, args.max_lines, args.backfill_days)
    elif args.cmd == "stream":
        cmd_stream(args.eps, args.apply, args.dsn)
    elif args.cmd == "live":
        cmd_live(
            eps=args.eps,
            reprice_chance=args.reprice_chance,
            reprice_cooldown=args.reprice_cooldown,
            reprice_count=args.reprice_count,
            reprice_pct_min=args.reprice_pct_min,
            reprice_pct_max=args.reprice_pct_max,
            reprice_direction=args.reprice_direction,
            reprice_only_active=args.reprice_only_active,
            apply=args.apply,
            dsn=args.dsn,
        )
    elif args.cmd == "reprice":
        cmd_reprice(
            apply=args.apply,
            dsn=args.dsn,
            outfile=args.outfile,
            pct_min=args.pct_min,
            pct_max=args.pct_max,
            direction=args.direction,
            count=args.count,
            per_day=args.per_day,
            backfill_start=args.backfill_start,
            backfill_end=args.backfill_end,
            only_active=args.only_active,
            scd2_log_csv=args.scd2_log_csv,
        )
    else:
        cmd_stream(args.eps, args.apply, args.dsn)

if __name__ == "__main__":
    main()
