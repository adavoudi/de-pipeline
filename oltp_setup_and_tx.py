#!/usr/bin/env python3
"""
MySQL OLTP Setup & Transactions

Implements the minimal e-commerce schema and generates realistic transactional data.
- init: create tables
- seed: insert customers & products
- orders: create orders & order_items with consistent currency math
- stream: continuously create orders

By default prints SQL. Use --apply with a DSN to execute against MySQL.
DSN format: "host=localhost user=root password=secret database=shop port=3306"
Requires: mysql-connector-python (only when using --apply)
"""

import argparse
import os
import random
import string
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from constants import (
    CATEGORIES,
    CHANNEL_PRIORS,
    CURRENCY,
    EU_COUNTRIES,
    MARKETING_CAMPAIGNS as CAMPAIGNS,
    SCHEMA_SQL,
    STATUSES,
)

from utils import parse_date, weighted_choice

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

def sample_name():
    first = random.choice(["Alex","Sam","Chris","Taylor","Jordan","Jamie","Robin","Avery","Kai","Riley","Max","Nico","Dana","Quinn","Noa"])
    last = random.choice(["Müller","Schmidt","Fischer","Weber","Schneider","Becker","Wagner","Hansen","Dubois","Rossi","Santos","Costa","Nowak","Kowalski","Ivanov"])
    return f"{first} {last}"

def slugify(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in s).strip("-")

def sample_email(full_name: str, i: int):
    base = slugify(full_name.replace(" ", "."))
    domain = random.choice(["example.com","shopmail.eu","email.test"])
    return f"{base}.{i}@{domain}"

def sample_product(pid: int) -> Tuple[str, str, str, float]:
    category = random.choice(CATEGORIES)
    name = f"{category} Item {pid}"
    product_id = f"p_{1000+pid}"
    # category-dependent price bands (EUR)
    base = {
        "Home": (15, 120),
        "Accessories": (10, 80),
        "Electronics": (30, 400),
        "Apparel": (12, 150),
        "Beauty": (8, 90),
    }[category]
    price = round(random.uniform(*base), 2)
    return product_id, name, category, price

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

def status_from_total(total: float) -> str:
    r = random.random()
    if r < 0.70:
        return "paid"
    elif r < 0.85:
        return "shipped"
    elif r < 0.92:
        return "created"
    elif r < 0.97:
        return "cancelled"
    else:
        return "refunded"

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
        from random import randint
        for i in range(1, 31):
            pid, _name, _cat, price = sample_product(i)
            products.append((pid, price, True))

    if only_active:
        products = [p for p in products if p[2]]

    if not products:
        raise RuntimeError("No products available to reprice.")

    # Build update plan (timestamp, product_id, old_price, new_price)
    plan: List[Tuple[str, str, float, float]] = []
    rng = random.Random()

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

def cmd_seed(customers: int, products: int, apply: bool, dsn: Optional[str], outfile: Optional[Path]) -> None:
    statements: List[str] = []

    # Customers
    cust_ids: List[str] = []
    for i in range(1, customers + 1):
        cid = f"c_{i:03d}"
        cust_ids.append(cid)
        full_name = sample_name()
        email = sample_email(full_name, i)
        country = random.choice(EU_COUNTRIES)
        opt_in = "TRUE" if random.random() < 0.35 else "FALSE"
        created = iso_ts(now_utc() - timedelta(days=random.randint(1, 120)))
        statements.append(
            f"insert into customers (customer_id,email,full_name,country_code,marketing_opt_in,created_at) "
            f"values ('{cid}','{email}','{full_name}','{country}',{opt_in},'{created}')"
        )

    # Products
    product_price_map: Dict[str,float] = {}
    for i in range(1, products + 1):
        pid, name, category, price = sample_product(i)
        product_price_map[pid] = price
        statements.append(
            f"insert into products (product_id,name,category,price,currency,is_active,updated_at) "
            f"values ('{pid}','{name}','{category}',{price:.2f},'{CURRENCY}',TRUE,'{iso_ts(now_utc())}')"
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
        customer_ids = [f"c_{i:03d}" for i in range(1, 501)]
        # simple default country for tax calc when printing SQL
        cust_country = {cid: random.choice(EU_COUNTRIES) for cid in customer_ids}
        for i in range(1, 31):
            pid, _, _, price = sample_product(i)
            product_ids_prices[pid] = price

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
            "subtotal,tax,shipping_fee,total_amount,channel,campaign_id,created_at"
        )
        order_values = (
            f"'{order_id}','{cid}','{iso_ts(order_ts)}','{status}','{CURRENCY}',"
            f"{subtotal:.2f},{tax:.2f},{shipping_fee:.2f},{total_amount:.2f},"
            f"'{channel}',{campaign_value},'{created_at}'"
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

                cur.execute(
                    "insert into orders (order_id,customer_id,order_ts,status,currency,subtotal,tax,shipping_fee,total_amount,channel,campaign_id,created_at) "
                    "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())",
                    (order_id, cid, order_ts, status, CURRENCY, subtotal, tax, shipping_fee, total_amount, channel, campaign_id)
                )
                for pid, qty, unit_price, line_amount in items:
                    cur.execute(
                        "insert into order_items (order_id,product_id,qty,unit_price,line_amount) values (%s,%s,%s,%s,%s)",
                        (order_id, pid, qty, unit_price, line_amount)
                    )
                conn.commit()
                print(f"Inserted order {order_id} ({len(items)} lines) total={total_amount:.2f} {CURRENCY}")
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Stopping stream...")
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
    p_seed.add_argument("--customers", type=int, default=500, help="Number of customers (default 500)")
    p_seed.add_argument("--products", type=int, default=30, help="Number of products (default 30)")
    # orders
    p_orders = sub.add_parser("orders", help="Generate orders & order_items")
    p_orders.add_argument("--count", type=int, default=200, help="Number of orders to create")
    p_orders.add_argument("--min-lines", type=int, default=1, help="Minimum lines per order")
    p_orders.add_argument("--max-lines", type=int, default=3, help="Maximum lines per order")
    p_orders.add_argument("--backfill-days", type=int, default=14, help="Spread order_ts across last N days")
    # stream
    p_stream = sub.add_parser("stream", help="Continuously insert orders")
    p_stream.add_argument("--eps", type=float, default=0.2, help="Approx orders per second (0..1 reasonable)")

    args = ap.parse_args()

    if args.cmd == "init":
        cmd_init(args.apply, args.dsn, args.outfile)
    elif args.cmd == "seed":
        cmd_seed(args.customers, args.products, args.apply, args.dsn, args.outfile)
    elif args.cmd == "orders":
        cmd_orders(args.count, args.apply, args.dsn, args.outfile, args.min_lines, args.max_lines, args.backfill_days)
    elif args.cmd == "stream":
        cmd_stream(args.eps, args.apply, args.dsn)
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
