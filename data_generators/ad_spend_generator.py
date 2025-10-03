#!/usr/bin/env python3
"""
Ad Spend Daily CSV Generator

Emits one CSV per day named: ad_spend_YYYY-MM-DD.csv

Columns:
date,channel,campaign_id,cost,impressions,clicks,currency

Example:
2025-09-29,google,cmp_42,120.50,10000,320,EUR

Usage:
  # Single day to stdout
  python3 ad_spend_generator.py --date 2025-09-29

  # Date range to a directory (files per day)
  python3 ad_spend_generator.py --start 2025-09-01 --end 2025-09-30 --outdir ./feeds

  # Control budgets and determinism
  python3 ad_spend_generator.py --date 2025-09-29 --daily-budget 350 --seed 42

Notes:
- Only paid channels are emitted by default: google, facebook, email.
- Campaign sets per channel mirror the other generators.
- Metrics are derived from simple CPM/CPC priors with noise and internal consistency: clicks <= impressions.
"""

import argparse
import csv
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import sys

from constants import (
    ALLOC_PRIORS,
    CPC_PRIORS,
    CPM_PRIORS,
    CURRENCY,
    PAID_CHANNELS,
    AD_SPEND_CAMPAIGNS as CAMPAIGNS,
)

from utils import parse_date

def daterange(start: datetime, end: datetime) -> Iterable[datetime]:
    # inclusive of end
    days = (end - start).days
    for i in range(days + 1):
        yield start + timedelta(days=i)

def softmax(vals: Dict[str, float]) -> Dict[str, float]:
    mx = max(vals.values())
    exps = {k: math.exp(v - mx) for k, v in vals.items()}
    s = sum(exps.values())
    return {k: v / s for k, v in exps.items()}

def allocate_budget(total: float) -> Dict[str, float]:
    # add day-to-day wiggle to allocation
    noise = {ch: math.log(ALLOC_PRIORS[ch]) + random.normalvariate(0, 0.2) for ch in PAID_CHANNELS}
    w = softmax(noise)
    return {ch: round(total * w[ch], 2) for ch in PAID_CHANNELS}

def bounded_int(val: float, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(round(val))))

def sample_metrics_for_channel(channel: str, cost: float) -> Tuple[int, int]:
    """Return (impressions, clicks) consistent with cost."""
    # derive impressions from CPM with noise
    cpm = max(0.5, random.normalvariate(CPM_PRIORS[channel], CPM_PRIORS[channel] * 0.2))
    impressions = 0 if cost <= 0 else bounded_int((cost / cpm) * 1000.0, 0, 5_000_000)

    # derive clicks from CPC with noise; ensure clicks <= impressions and sensible CTR
    cpc = max(0.05, random.normalvariate(CPC_PRIORS[channel], CPC_PRIORS[channel] * 0.25))
    clicks = 0 if cost <= 0 else bounded_int(cost / cpc, 0, impressions)

    # additional CTR sanity (0.1% .. 12% typical bounds)
    if impressions > 0:
        ctr = clicks / impressions
        min_ctr, max_ctr = 0.001, 0.12
        if ctr < min_ctr:
            clicks = bounded_int(impressions * min_ctr, 0, impressions)
        elif ctr > max_ctr:
            clicks = bounded_int(impressions * max_ctr, 0, impressions)

    return impressions, clicks

def distribute_campaigns(channel: str, channel_budget: float) -> Dict[str, float]:
    # Split channel budget across campaigns with Dirichlet-like noise
    camps = CAMPAIGNS[channel]
    weights = [random.gammavariate(2.0, 1.0) for _ in camps]
    s = sum(weights)
    allocs = [round(channel_budget * w / s, 2) for w in weights]
    # Adjust rounding drift
    drift = round(channel_budget - sum(allocs), 2)
    if allocs:
        allocs[0] = round(allocs[0] + drift, 2)
    return dict(zip(camps, allocs))

def rows_for_day(day: datetime, daily_budget: float) -> List[List[str]]:
    rows: List[List[str]] = []
    by_channel = allocate_budget(daily_budget)
    for ch, ch_budget in by_channel.items():
        by_campaign = distribute_campaigns(ch, ch_budget)
        for camp, cost in by_campaign.items():
            impressions, clicks = sample_metrics_for_channel(ch, cost)
            rows.append([
                day.strftime("%Y-%m-%d"),
                ch,
                camp,
                f"{cost:.2f}",
                str(impressions),
                str(clicks),
                CURRENCY,
            ])
    return rows

def write_csv(path: Path, rows: List[List[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","channel","campaign_id","cost","impressions","clicks","currency"])
        w.writerows(rows)

def main():
    ap = argparse.ArgumentParser(description="Generate daily ad spend CSV(s)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="Single date YYYY-MM-DD")
    g.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", help="End date YYYY-MM-DD (inclusive; required with --start)")
    ap.add_argument("--outdir", type=Path, help="Directory to write files (default: stdout)")
    ap.add_argument("--daily-budget", type=float, default=300.0, help="Total EUR budget per day to allocate")
    ap.add_argument("--seed", type=int, help="Random seed for reproducibility")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.date:
        days = [parse_date(args.date)]
    else:
        if not args.end:
            ap.error("--end is required when using --start")
        days = list(daterange(parse_date(args.start), parse_date(args.end)))

    for day in days:
        rows = rows_for_day(day, args.daily_budget)
        if args.outdir:
            args.outdir.mkdir(parents=True, exist_ok=True)
            path = args.outdir / f"ad_spend_{day.strftime('%Y-%m-%d')}.csv"
            write_csv(path, rows)
            print(f"Wrote {path}")
        else:
            # stdout single file
            w = csv.writer(sys.stdout)
            # header once per day (since often used one day at a time on stdout)
            print("date,channel,campaign_id,cost,impressions,clicks,currency")
            w.writerows(rows)

if __name__ == "__main__":
    main()
