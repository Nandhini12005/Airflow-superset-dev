"""Configuration for the Maven ETL DAG.

This module holds the constants and table configuration so the main DAG
file can remain focused on task wiring.
"""
from __future__ import annotations

import os
from pathlib import Path

CONN_ID = "warehouse"
DATA_DIR = Path(os.environ.get("BOOTCAMP_DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"

TABLES: dict[str, dict] = {
    "website_sessions": {
        "csv": "website_sessions.csv",
        "pk": "website_session_id",
        "columns": ["website_session_id", "created_at", "user_id", "is_repeat_session",
                    "utm_source", "utm_campaign", "utm_content", "device_type", "http_referer"],
        "min_rows": 400_000,
    },
    "website_pageviews": {
        "csv": "website_pageviews.csv",
        "pk": "website_pageview_id",
        "columns": ["website_pageview_id", "created_at", "website_session_id", "pageview_url"],
        "min_rows": 1_000_000,
    },
    "orders": {
        "csv": "orders.csv",
        "pk": "order_id",
        "columns": ["order_id", "created_at", "website_session_id", "user_id",
                    "primary_product_id", "items_purchased", "price_usd", "cogs_usd"],
        "min_rows": 25_000,
    },
    "order_items": {
        "csv": "order_items.csv",
        "pk": "order_item_id",
        "columns": ["order_item_id", "created_at", "order_id", "product_id",
                    "is_primary_item", "price_usd", "cogs_usd"],
        "min_rows": 25_000,
    },
    "order_item_refunds": {
        "csv": "order_item_refunds.csv",
        "pk": "order_item_refund_id",
        "columns": ["order_item_refund_id", "created_at", "order_item_id",
                    "order_id", "refund_amount_usd"],
        "min_rows": 100,
    },
    "products": {
        "csv": "products.csv",
        "pk": "product_id",
        "columns": ["product_id", "created_at", "product_name"],
        "min_rows": 1,
    },
}

FOREIGN_KEYS = [
    ("orders", "website_session_id", "website_sessions", "website_session_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("website_pageviews", "website_session_id", "website_sessions", "website_session_id"),
]
