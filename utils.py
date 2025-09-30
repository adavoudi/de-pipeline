"""Utilities shared across generator scripts."""

import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from constants import CATEGORIES, EU_COUNTRIES

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


DEFAULT_SEED = 1234
_FIRST_NAMES = [
    "Alex", "Sam", "Chris", "Taylor", "Jordan", "Jamie", "Robin",
    "Avery", "Kai", "Riley", "Max", "Nico", "Dana", "Quinn", "Noa",
]
_LAST_NAMES = [
    "Müller", "Schmidt", "Fischer", "Weber", "Schneider", "Becker",
    "Wagner", "Hansen", "Dubois", "Rossi", "Santos", "Costa", "Nowak",
    "Kowalski", "Ivanov",
]
_EMAIL_DOMAINS = ["example.com", "shopmail.eu", "email.test"]
_CATEGORY_PRICE_RANGES = {
    "Home": (15, 120),
    "Accessories": (10, 80),
    "Electronics": (30, 400),
    "Apparel": (12, 150),
    "Beauty": (8, 90),
}


def slugify(value: str) -> str:
    """Convert a string to a lowercase ASCII slug."""
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")


def _resolve_rng(seed: Optional[int]) -> random.Random:
    return random.Random(DEFAULT_SEED if seed is None else seed)


def generate_customers(
    count: int,
    *,
    seed: Optional[int] = None,
    start_index: int = 1,
    reference_time: Optional[datetime] = None,
) -> List[Dict[str, object]]:
    """Generate deterministic customer records for seeding or reference data."""
    if count <= 0:
        return []
    rng = _resolve_rng(seed)
    base_time = reference_time or datetime.now(timezone.utc)
    customers: List[Dict[str, object]] = []
    for offset in range(count):
        idx = start_index + offset
        full_name = f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"
        email_base = slugify(full_name.replace(" ", "."))
        domain = rng.choice(_EMAIL_DOMAINS)
        email = f"{email_base}.{idx}@{domain}"
        country = rng.choice(EU_COUNTRIES)
        marketing_opt_in = rng.random() < 0.35
        created_at = base_time - timedelta(days=rng.randint(1, 120))
        customers.append(
            {
                "customer_id": f"c_{idx:03d}",
                "full_name": full_name,
                "email": email,
                "country_code": country,
                "marketing_opt_in": marketing_opt_in,
                "created_at": created_at,
            }
        )
    return customers


def generate_products(
    count: int,
    *,
    seed: Optional[int] = None,
    start_index: int = 1,
) -> List[Dict[str, object]]:
    """Generate deterministic product catalog entries."""
    if count <= 0:
        return []
    rng = _resolve_rng(seed)
    products: List[Dict[str, object]] = []
    for offset in range(count):
        idx = start_index + offset
        category = rng.choice(CATEGORIES)
        min_price, max_price = _CATEGORY_PRICE_RANGES[category]
        price = round(rng.uniform(min_price, max_price), 2)
        products.append(
            {
                "product_id": f"p_{1000 + idx}",
                "name": f"{category} Item {idx}",
                "category": category,
                "price": price,
            }
        )
    return products
