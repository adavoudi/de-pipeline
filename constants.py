"""Shared constants used across data generation scripts."""

# Currency shared across datasets
CURRENCY = "EUR"

# --- Ad spend generator ---
PAID_CHANNELS = ["google", "facebook", "email"]
AD_SPEND_CAMPAIGNS = {
    "google": ["cmp_42", "cmp_generic", "cmp_brand"],
    "facebook": ["cmp_21", "cmp_ret"],
    "email": ["cmp_news", "cmp_promo"],
}
CPM_PRIORS = {
    "google": 3.5,
    "facebook": 5.0,
    "email": 1.2,
}
CPC_PRIORS = {
    "google": 0.55,
    "facebook": 0.42,
    "email": 0.08,
}
ALLOC_PRIORS = {
    "google": 0.45,
    "facebook": 0.40,
    "email": 0.15,
}

# --- OLTP setup & transactions ---
SCHEMA_SQL = r"""
-- customers (PII kept minimal)
create table if not exists customers (
  customer_id varchar(36) primary key,
  email varchar(255) unique not null,
  full_name varchar(200) not null,
  country_code char(2) not null,
  marketing_opt_in boolean not null default false,
  created_at timestamp not null default current_timestamp
);

-- products (price kept here; changes over time -> drives SCD2 in warehouse)
create table if not exists products (
  product_id varchar(30) primary key,
  name varchar(200) not null,
  category varchar(100) not null,
  price numeric(10,2) not null,
  currency char(3) not null,
  is_active boolean not null default true,
  updated_at timestamp not null default current_timestamp
);

-- orders (minimal but analytic-ready)
create table if not exists orders (
  order_id varchar(36) primary key,
  customer_id varchar(36) not null,
  order_ts timestamp not null,
  status varchar(20) not null,
  currency char(3) not null,
  subtotal numeric(12,2) not null,
  tax numeric(12,2) not null,
  shipping_fee numeric(12,2) not null,
  total_amount numeric(12,2) not null,
  channel varchar(20),
  campaign_id varchar(50),
  created_at timestamp not null default current_timestamp,
  constraint fk_orders_customer foreign key (customer_id) references customers(customer_id),
  constraint chk_status check (status in ('created','paid','shipped','cancelled','refunded'))
);

-- order_items (one row per product in the order)
create table if not exists order_items (
  order_id varchar(36) not null,
  product_id varchar(30) not null,
  qty int not null,
  unit_price numeric(10,2) not null,
  line_amount numeric(12,2) not null,
  primary key (order_id, product_id),
  constraint fk_items_order foreign key (order_id) references orders(order_id),
  constraint fk_items_product foreign key (product_id) references products(product_id),
  constraint chk_qty check (qty > 0)
);
"""

EU_COUNTRIES = ["DE","FR","ES","IT","NL","BE","AT","SE","DK","PL"]
CATEGORIES = ["Home", "Accessories", "Electronics", "Apparel", "Beauty"]
CUSTOMER_COUNT = 500
PRODUCT_COUNT = 30
CHANNEL_PRIORS = {
    "google": 0.38,
    "facebook": 0.27,
    "email": 0.10,
    "direct": 0.20,
    "other": 0.05,
}
MARKETING_CAMPAIGNS = {
    "google": ["cmp_42", "cmp_generic", "cmp_brand", None],
    "facebook": ["cmp_21", "cmp_ret", None],
    "email": ["cmp_news", "cmp_promo", None],
    "direct": [None],
    "other": [None, "cmp_aff"],
}
STATUSES = ["created","paid","shipped","cancelled","refunded"]

# --- Clickstream generator ---
ANON_RATE = 0.45
MEAN_VIEWS_PER_SESSION = 2.4
P_ADD_TO_CART = 0.30
P_CHECKOUT_FROM_ATC = 0.55
MAX_SESSION_SECONDS = 900
