"""Utilities shared across generator scripts."""

import random
from datetime import datetime
from typing import Dict

def parse_date(value: str, fmt: str = "%Y-%m-%d") -> datetime:
    """Parse a date string using the provided format (default YYYY-MM-DD)."""
    return datetime.strptime(value, fmt)

def weighted_choice(weights: Dict[str, float]) -> str:
    """Select a key based on weighted probability (weights need not be normalized)."""
    if not weights:
        raise ValueError("weights dictionary must not be empty")
    threshold = random.random() * sum(weights.values())
    cumulative = 0.0
    for key, weight in weights.items():
        cumulative += weight
        if cumulative >= threshold:
            return key
    # Fallback in case of floating point drift
    return next(iter(weights))
