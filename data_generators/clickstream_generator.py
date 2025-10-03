#!/usr/bin/env python3
"""
Clickstream Events Generator

Generates realistic clickstream events to stdout, a JSONL file, or an HTTP(S) endpoint.
Schema (one event type): clickstream_events

{
  "event_id": "uuid",
  "event_ts": "2025-09-29T12:34:56Z",
  "session_id": "s_abc123",
  "customer_id": "c_001",                 # nullable (anonymous browsing)
  "event_type": "product_view|add_to_cart|checkout_started",
  "product_id": "p_1001",                 # nullable except for product events
  "channel": "google|facebook|email|direct|other",
  "campaign_id": "cmp_42",                # nullable
  "currency": "EUR",
  "ingest_ts": "2025-09-29T12:34:57Z"
}

Why these fields?
- event_id enables idempotent writes + dedup
- session_id supports building fct_sessions
- channel/campaign_id enables attribution without extra sources
- Minimal event_type set supports funnel math
"""

import argparse
import json
import math
import random
import sys
import time
import uuid
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from constants import (
    ANON_RATE,
    CHANNEL_PRIORS,
    CURRENCY,
    CUSTOMER_COUNT,
    MARKETING_CAMPAIGNS as CAMPAIGNS,
    MEAN_VIEWS_PER_SESSION,
    P_ADD_TO_CART,
    P_CHECKOUT_FROM_ATC,
    PRODUCT_COUNT,
)

from utils import DEFAULT_SEED, generate_customers, generate_products, weighted_choice

random.seed(DEFAULT_SEED)

CUSTOMER_RECORDS = generate_customers(CUSTOMER_COUNT)
CUSTOMER_IDS = [c["customer_id"] for c in CUSTOMER_RECORDS]
PRODUCT_CATALOG = generate_products(PRODUCT_COUNT)
PRODUCT_IDS = [p["product_id"] for p in PRODUCT_CATALOG]

# ---------- Realistic reference data ----------
# Inter-event timing (to make timestamps look real)
def sample_interaction_delay() -> float:
    """Seconds between interactions: log-normal-ish mix."""
    base = random.lognormvariate(mu=1.0, sigma=0.6)  # median ~e^1=2.7s
    # occasional longer thinking time
    if random.random() < 0.05:
        base *= 5
    return min(base, 60.0)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def poisson(lam: float) -> int:
    # Knuth's algorithm for small lam
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return max(0, k - 1)

# ---------- Session generation ----------

def new_session(now_utc: datetime) -> Dict:
    session_id = f"s_{uuid.uuid4().hex[:8]}"
    channel = weighted_choice(CHANNEL_PRIORS)
    campaign_id = random.choice(CAMPAIGNS[channel])
    is_anon = random.random() < ANON_RATE
    customer_id = None if is_anon else random.choice(CUSTOMER_IDS)
    # draws number of product views (>=1)
    views = max(1, poisson(MEAN_VIEWS_PER_SESSION))
    # Determine funnel path
    did_atc = (random.random() < P_ADD_TO_CART)
    did_checkout = did_atc and (random.random() < P_CHECKOUT_FROM_ATC)
    steps = ["product_view"] * views
    if did_atc:
        steps.append("add_to_cart")
    if did_checkout:
        steps.append("checkout_started")
    # Assign products to each step (views may vary; ATC/checkout tied to last viewed product)
    products = [random.choice(PRODUCT_IDS) for _ in range(views)]
    last_product = products[-1]
    if did_atc:
        products.append(last_product)
    if did_checkout:
        products.append(last_product)
    return {
        "session_id": session_id,
        "channel": channel,
        "campaign_id": campaign_id,
        "customer_id": customer_id,
        "steps": steps,
        "products": products,
        "start_time": now_utc
    }

def iter_session_events(sess: Dict) -> Iterator[Dict]:
    t = sess["start_time"]
    for step, product_id in zip(sess["steps"], sess["products"]):
        # advance "thinking" time
        t += timedelta(seconds=sample_interaction_delay())
        event = {
            "event_id": str(uuid.uuid4()),
            "event_ts": iso_z(t),
            "session_id": sess["session_id"],
            "customer_id": sess["customer_id"],
            "event_type": step,
            "product_id": product_id if step in ("product_view", "add_to_cart", "checkout_started") else None,
            "channel": sess["channel"],
            "campaign_id": sess["campaign_id"],
            "currency": CURRENCY,
            "ingest_ts": iso_z(datetime.now(timezone.utc))
        }
        yield event

# ---------- Emitters ----------

def emit_stdout(events: List[Dict]) -> None:
    for e in events:
        sys.stdout.write(json.dumps(e, separators=(",", ":"), ensure_ascii=False) + "\n")
    sys.stdout.flush()

def emit_file(path: Path, events: List[Dict]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, separators=(",", ":"), ensure_ascii=False) + "\n")

def emit_http(endpoint: str, events: List[Dict]) -> None:
    headers = {"Content-Type": "application/json"}
    for e in events:
        payload = json.dumps(e, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=10):
                pass
        except urllib.error.HTTPError as exc:
            sys.stderr.write(f"HTTP error {exc.code} while sending event {e['event_id']}: {exc.reason}\n")
        except urllib.error.URLError as exc:
            sys.stderr.write(f"Failed to reach {endpoint} for event {e['event_id']}: {exc.reason}\n")

# ---------- Main loop ----------

def generate_events_stream(total_events: Optional[int], eps: float, output, http_endpoint: Optional[str], jitter: float) -> None:
    """
    total_events: stop after this many events (None -> run forever)
    eps: events per second target (global)
    output: None -> stdout, Path -> JSONL file
    http_endpoint: URL to POST events to, if provided
    jitter: +/- fraction to vary eps each second (e.g., 0.25 = ±25%)
    """
    random.seed(DEFAULT_SEED)
    emitted = 0
    # slightly staggered start times per session
    base_time = datetime.now(timezone.utc)

    # control loop timer
    next_tick = time.perf_counter()
    interval = 1.0

    while (total_events is None) or (emitted < total_events):
        # Decide how many sessions to start this second and how many events to emit
        # Approximate: target eps; average events per session ~= MEAN_VIEWS_PER_SESSION + P_ADD_TO_CART + P_ADD_TO_CART*P_CHECKOUT_FROM_ATC
        avg_events_per_session = MEAN_VIEWS_PER_SESSION + P_ADD_TO_CART + (P_ADD_TO_CART * P_CHECKOUT_FROM_ATC)
        target_eps = eps * (1.0 + random.uniform(-jitter, jitter))
        sessions_this_tick = max(1, int(target_eps / max(0.1, avg_events_per_session)))

        batch: List[Dict] = []
        for _ in range(sessions_this_tick):
            sess = new_session(base_time)
            for evt in iter_session_events(sess):
                batch.append(evt)

        # Trim batch if it overshoots remaining quota
        if total_events is not None and emitted + len(batch) > total_events:
            batch = batch[: max(0, total_events - emitted)]

        # Emit
        if http_endpoint is not None:
            emit_http(http_endpoint, batch)
        elif output is None:
            emit_stdout(batch)
        else:
            emit_file(output, batch)

        emitted += len(batch)

        # Pace to roughly match eps
        next_tick += interval
        sleep_for = next_tick - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)

def parse_args():
    ap = argparse.ArgumentParser(description="Realistic clickstream events generator")
    g_out = ap.add_mutually_exclusive_group()
    g_out.add_argument("--outfile", type=Path, help="Write JSONL to this file (appends)")
    g_out.add_argument("--stdout", action="store_true", help="Write to stdout (default)")
    ap.add_argument("--http-endpoint", help="HTTP(S) endpoint to POST events to")
    ap.add_argument("--events", type=int, help="Total number of events to emit (default: run forever)")
    ap.add_argument("--eps", type=float, default=20.0, help="Target events per second (default: 20)")
    ap.add_argument("--jitter", type=float, default=0.25, help="Per-second EPS jitter fraction (default: 0.25)")
    return ap.parse_args()

def main():
    args = parse_args()
    output = None
    http_endpoint = args.http_endpoint

    if args.outfile and not http_endpoint:
        output = args.outfile
    else:
        output = None  # stdout

    try:
        generate_events_stream(
            total_events=args.events,
            eps=args.eps,
            output=output,
            http_endpoint=http_endpoint,
            jitter=args.jitter,
        )
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
