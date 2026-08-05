-- Day 2 warehouse schema: raw lands in STAGING, clean marts live in ANALYTICS.
-- Run once against the `warehouse` database before the first pipeline run:
--   psql -h localhost -U airflow -d warehouse -f warehouse_schema.sql

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ------------------------------------------------------------------ STAGING --
-- Verbatim landing tables. Types are permissive on purpose; cleaning happened
-- in pandas, and keeping staging forgiving makes a bad batch land (and fail the
-- gate) rather than error on COPY.

DROP TABLE IF EXISTS staging.website_sessions;
CREATE TABLE staging.website_sessions (
    website_session_id  BIGINT,
    created_at          TIMESTAMP,
    user_id             BIGINT,
    is_repeat_session   INTEGER,
    utm_source          TEXT,
    utm_campaign        TEXT,
    utm_content         TEXT,
    device_type         TEXT,
    http_referer        TEXT
);

DROP TABLE IF EXISTS staging.website_pageviews;
CREATE TABLE staging.website_pageviews (
    website_pageview_id BIGINT,
    created_at          TIMESTAMP,
    website_session_id  BIGINT,
    pageview_url        TEXT
);

DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders (
    order_id            BIGINT,
    created_at          TIMESTAMP,
    website_session_id  BIGINT,
    user_id             BIGINT,
    primary_product_id  INTEGER,
    items_purchased     INTEGER,
    price_usd           NUMERIC(10, 2),
    cogs_usd            NUMERIC(10, 2)
);

DROP TABLE IF EXISTS staging.order_items;
CREATE TABLE staging.order_items (
    order_item_id       BIGINT,
    created_at          TIMESTAMP,
    order_id            BIGINT,
    product_id          INTEGER,
    is_primary_item     INTEGER,
    price_usd           NUMERIC(10, 2),
    cogs_usd            NUMERIC(10, 2)
);

DROP TABLE IF EXISTS staging.order_item_refunds;
CREATE TABLE staging.order_item_refunds (
    order_item_refund_id BIGINT,
    created_at           TIMESTAMP,
    order_item_id        BIGINT,
    order_id             BIGINT,
    refund_amount_usd    NUMERIC(10, 2)
);

DROP TABLE IF EXISTS staging.products;
CREATE TABLE staging.products (
    product_id   INTEGER,
    created_at   TIMESTAMP,
    product_name TEXT
);

-- ---------------------------------------------------------------- ANALYTICS --
-- Clean, typed marts. These are the only tables Superset reads on Day 3.

CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_id   INTEGER PRIMARY KEY,
    created_at   TIMESTAMP,
    product_name TEXT
);

CREATE TABLE IF NOT EXISTS analytics.fct_orders (
    order_id           BIGINT PRIMARY KEY,
    created_at         TIMESTAMP,
    website_session_id BIGINT,
    primary_product_id INTEGER,
    items_purchased    INTEGER,
    price_usd          NUMERIC(10, 2),
    cogs_usd           NUMERIC(10, 2),
    margin_usd         NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS analytics.fct_sessions (
    website_session_id BIGINT PRIMARY KEY,
    created_at         TIMESTAMP,
    utm_source         TEXT,
    channel            TEXT,
    device_type        TEXT,
    is_converted       BOOLEAN
);

-- Helpful indexes for the Day 3 dashboards.
CREATE INDEX IF NOT EXISTS idx_fct_orders_created  ON analytics.fct_orders (created_at);
CREATE INDEX IF NOT EXISTS idx_fct_sessions_channel ON analytics.fct_sessions (channel);
