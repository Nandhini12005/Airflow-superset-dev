"""
Day 2 - Production-Ready ETL Pipeline
=====================================
Extract the Maven Fuzzy Factory CSVs, clean them with pandas, gate the load
behind data-quality checks, and bulk-load the result into PostgreSQL.

Flow:  extract_and_stage  ->  validate  ->  (branch)  ->  build_analytics  ->  verify
                                                    \\-> quarantine

Requires an Airflow connection called ``warehouse`` pointing at the analytics
PostgreSQL database. See README.md for the one-line command that creates it.
"""
from __future__ import annotations

import csv
import io
import os
from pathlib import Path

import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from maven_etl_config import CONN_ID, DATA_DIR, RAW_DIR, QUARANTINE_DIR, TABLES, FOREIGN_KEYS
from maven_etl_helpers import clean_frame, copy_into_staging, scalar

# Configuration and helpers moved into dedicated modules:
#  - dags/maven_etl_config.py
#  - dags/maven_etl_helpers.py


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
@dag(
    dag_id="maven_etl",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["day2", "etl", "maven"],
    doc_md=__doc__,
)
def maven_etl():

    @task
    def extract_and_stage(table: str) -> dict:
        """Read one raw CSV, clean it, and COPY it into its staging table."""
        cfg = TABLES[table]
        path = RAW_DIR / cfg["csv"]
        if not path.exists():
            raise AirflowException(f"Missing raw file: {path}")

        # Chunked read keeps memory flat even on the ~1.19M-row pageviews file.
        frames = [clean_frame(chunk, cfg)
                  for chunk in pd.read_csv(path, chunksize=200_000, low_memory=False)]
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=[cfg["pk"]])

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        copy_into_staging(df, table, hook)
        return {"table": table, "rows": int(len(df))}

    @task.branch
    def validate(stage_results: list[dict]) -> str:
        """Run row-count, uniqueness and referential checks; decide load vs quarantine."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        problems: list[str] = []

        # 1) Row-count thresholds + primary-key uniqueness.
        for table, cfg in TABLES.items():
            n = scalar(hook, f"SELECT count(*) FROM staging.{table};")
            if n < cfg["min_rows"]:
                problems.append(f"{table}: {n} rows < min {cfg['min_rows']}")
            dupes = scalar(
                hook,
                f"SELECT count(*) FROM (SELECT {cfg['pk']} FROM staging.{table} "
                f"GROUP BY {cfg['pk']} HAVING count(*) > 1) d;",
            )
            if dupes:
                problems.append(f"{table}: {dupes} duplicate {cfg['pk']} values")

        # 2) Referential integrity.
        for child, ccol, parent, pcol in FOREIGN_KEYS:
            orphans = scalar(
                hook,
                f"SELECT count(*) FROM staging.{child} c "
                f"LEFT JOIN staging.{parent} p ON c.{ccol} = p.{pcol} "
                f"WHERE c.{ccol} IS NOT NULL AND p.{pcol} IS NULL;",
            )
            if orphans:
                problems.append(f"{child}.{ccol}: {orphans} orphan rows (no {parent}.{pcol})")

        if problems:
            print("DATA QUALITY GATE FAILED:\n  - " + "\n  - ".join(problems))
            return "quarantine"
        print("Data quality gate PASSED for all tables.")
        return "build_analytics"

    @task
    def build_analytics() -> None:
        """Transform staging into the analytics marts the dashboards read (idempotent)."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run(
            [
                # dim_products
                "TRUNCATE analytics.dim_products;",
                """INSERT INTO analytics.dim_products (product_id, created_at, product_name)
                   SELECT product_id, created_at, product_name FROM staging.products;""",
                # fct_orders (+ derived margin)
                "TRUNCATE analytics.fct_orders;",
                """INSERT INTO analytics.fct_orders
                       (order_id, created_at, website_session_id, primary_product_id,
                        items_purchased, price_usd, cogs_usd, margin_usd)
                   SELECT order_id, created_at, website_session_id, primary_product_id,
                          items_purchased, price_usd, cogs_usd,
                          price_usd - cogs_usd AS margin_usd
                   FROM staging.orders;""",
                # fct_sessions (+ channel grouping + converted flag)
                "TRUNCATE analytics.fct_sessions;",
                """INSERT INTO analytics.fct_sessions
                       (website_session_id, created_at, utm_source, channel,
                        device_type, is_converted)
                   SELECT s.website_session_id, s.created_at, s.utm_source,
                          CASE
                            WHEN s.utm_source IN ('gsearch', 'bsearch') THEN 'search'
                            WHEN s.utm_source = 'socialbook'            THEN 'social'
                            WHEN s.utm_source = 'direct'                THEN 'direct'
                            ELSE 'other'
                          END AS channel,
                          s.device_type,
                          (o.website_session_id IS NOT NULL) AS is_converted
                   FROM staging.website_sessions s
                   LEFT JOIN (SELECT DISTINCT website_session_id FROM staging.orders) o
                          ON s.website_session_id = o.website_session_id;""",
            ]
        )
        print("Analytics marts rebuilt: dim_products, fct_orders, fct_sessions.")

    @task
    def verify() -> None:
        """Read the marts back and log the headline KPIs, proving the load worked."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        orders = scalar(hook, "SELECT count(*) FROM analytics.fct_orders;")
        revenue = scalar(hook, "SELECT COALESCE(sum(price_usd), 0) FROM analytics.fct_orders;")
        margin = scalar(hook, "SELECT COALESCE(sum(margin_usd), 0) FROM analytics.fct_orders;")
        sessions = scalar(hook, "SELECT count(*) FROM analytics.fct_sessions;")
        converted = scalar(hook, "SELECT count(*) FROM analytics.fct_sessions WHERE is_converted;")
        conv = (converted / sessions * 100) if sessions else 0
        print(
            "VERIFY OK | "
            f"orders={orders:,} revenue=${revenue:,.0f} margin=${margin:,.0f} "
            f"sessions={sessions:,} conversion={conv:.2f}%"
        )

    @task(trigger_rule="none_failed_min_one_success")
    def quarantine() -> None:
        """Runs only when the gate fails: surface the failure loudly so no bad data loads."""
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        raise AirflowException(
            "Batch failed the data-quality gate and was NOT loaded. "
            f"Inspect the validate task logs; quarantine dir: {QUARANTINE_DIR}"
        )

    # ---- wiring -------------------------------------------------------------
    staged = extract_and_stage.expand(table=list(TABLES.keys()))
    decision = validate(staged)
    built = build_analytics()
    decision >> [built, quarantine()]
    built >> verify()


maven_etl()
