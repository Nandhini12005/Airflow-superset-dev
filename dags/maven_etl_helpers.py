"""Helper utilities for the Maven ETL DAG.

This module contains pure functions that can be unit-tested independently
from the Airflow tasks.
"""
from __future__ import annotations

import csv
import io

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def clean_frame(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]

    for col in df.columns:
        if col.endswith("_at"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif col.endswith("_usd"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "utm_source" in df.columns:
        df["utm_source"] = df["utm_source"].fillna("direct").str.strip().str.lower()
    if "utm_campaign" in df.columns:
        df["utm_campaign"] = df["utm_campaign"].fillna("none").str.strip().str.lower()
    if "utm_content" in df.columns:
        df["utm_content"] = df["utm_content"].fillna("none").str.strip().str.lower()
    if "device_type" in df.columns:
        df["device_type"] = df["device_type"].fillna("unknown").str.strip().str.lower()

    pk = cfg["pk"]
    df = df.dropna(subset=[pk]).drop_duplicates(subset=[pk])
    return df.reindex(columns=cfg["columns"])


def copy_into_staging(df: pd.DataFrame, table: str, hook: PostgresHook) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="", quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE staging.{table};")
            cur.copy_expert(f"COPY staging.{table} FROM STDIN WITH (FORMAT csv, NULL '')", buf)
        conn.commit()
    finally:
        conn.close()


def scalar(hook: PostgresHook, sql: str):
    return hook.get_first(sql)[0]
