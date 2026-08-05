#!/usr/bin/env python3
"""
Prepare the Maven Fuzzy Factory data for Day 2.

- Unzips the provided dataset into data/raw/  (the git-ignored landing zone)
- Writes small committable samples into data/samples/  (first 500 rows each)
- Optionally builds a deliberately BROKEN copy of orders.csv so the class can
  watch the data-quality gate reject a batch.

Usage:
    python scripts/prepare_data.py --zip Maven_Fuzzy_Factory.zip
    python scripts/prepare_data.py --break        # create the bad batch
"""
from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
SAMPLES = ROOT / "data" / "samples"
TABLES = [
    "website_sessions", "website_pageviews", "orders",
    "order_items", "order_item_refunds", "products",
]


def unzip(zip_path: Path) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        for member in z.namelist():
            target_name = Path(member).name
            if member.endswith(".csv") and not target_name.startswith("._"):
                # Flatten any nested folders in the archive into data/raw/.
                target = RAW / target_name
                with z.open(member) as src, open(target, "wb") as dst:
                    dst.write(src.read())
                print(f"  extracted {target.name}")


def write_samples() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    for name in TABLES:
        src = RAW / f"{name}.csv"
        if src.exists():
            pd.read_csv(src, nrows=500).to_csv(SAMPLES / f"{name}.csv", index=False)
            print(f"  sample -> data/samples/{name}.csv (500 rows)")


def make_broken_batch() -> None:
    """Duplicate 50 order_ids so the primary-key uniqueness check fails."""
    src = RAW / "orders.csv"
    df = pd.read_csv(src)
    broken = pd.concat([df, df.head(50)], ignore_index=True)
    broken.to_csv(src, index=False)
    print(f"  injected 50 duplicate order_id rows into {src.name} "
          f"({len(df):,} -> {len(broken):,} rows). Re-run the DAG to see the gate fail.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", type=Path, help="Path to the Maven dataset .zip")
    ap.add_argument("--break", dest="break_it", action="store_true",
                    help="Corrupt orders.csv to demo the quality gate")
    args = ap.parse_args()

    if args.break_it:
        make_broken_batch()
        return
    if not args.zip:
        ap.error("provide --zip PATH or --break")
    print(f"Unzipping {args.zip} -> {RAW}")
    unzip(args.zip)
    print("Writing samples ...")
    write_samples()
    print("Done. Full data is in data/raw/ (git-ignored); samples are committable.")


if __name__ == "__main__":
    main()
