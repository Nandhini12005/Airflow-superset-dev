import io
import os
import re
import zipfile
from pathlib import Path

try:
    import pandas as pd
    import psycopg2
    from psycopg2 import sql
except ImportError as exc:
    raise SystemExit(
        "pandas and psycopg2 are required. Install pandas and psycopg2-binary or run this through docker compose."
    ) from exc

zip_path = Path("Archive.zip")

# COPY is a single streamed bulk operation, so large chunks are fine (and faster).
CHUNK_SIZE = 50_000


def normalize_identifier(value: str, prefix: str = "item") -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z_]+", "_", value.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        return prefix
    if cleaned[0].isdigit():
        return f"{prefix}_{cleaned}"
    return cleaned


def unique_names(values):
    seen = set()
    result = []
    for index, value in enumerate(values, start=1):
        candidate = normalize_identifier(value, prefix=f"col_{index}")
        base = candidate
        suffix = 2
        while candidate in seen:
            candidate = f"{base}_{suffix}"
            suffix += 1
        seen.add(candidate)
        result.append(candidate)
    return result


def load_csv_files() -> None:
    if not zip_path.exists():
        raise SystemExit(f"Archive not found: {zip_path.resolve()}")

    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "superset"),
        user=os.environ.get("POSTGRES_USER", "superset"),
        password=os.environ.get("POSTGRES_PASSWORD", "superset"),
    )

    try:
        with connection, zipfile.ZipFile(zip_path) as archive:
            csv_members = [
                member
                for member in archive.infolist()
                if member.filename.endswith(".csv") and "__MACOSX" not in member.filename
            ]
            if not csv_members:
                print(f"No CSV files found in {zip_path.resolve()}", flush=True)
                return

            for member in csv_members:
                table_name = normalize_identifier(Path(member.filename).stem, prefix="table")
                with archive.open(member) as raw_handle:
                    with io.TextIOWrapper(
                        raw_handle, encoding="utf-8-sig", newline=""
                    ) as text_handle:
                        chunk_reader = pd.read_csv(
                            text_handle,
                            chunksize=CHUNK_SIZE,
                            dtype=str,
                            keep_default_na=False,
                        )

                        try:
                            first_chunk = next(chunk_reader)
                        except StopIteration:
                            print(f"Skipped empty file: {member.filename}", flush=True)
                            continue

                        headers = unique_names(first_chunk.columns)
                        quoted_table = sql.Identifier(table_name)
                        column_definitions = sql.SQL(", ").join(
                            sql.SQL("{} TEXT").format(sql.Identifier(column))
                            for column in headers
                        )

                        with connection.cursor() as cursor:
                            cursor.execute(
                                sql.SQL("DROP TABLE IF EXISTS {}").format(quoted_table)
                            )
                            cursor.execute(
                                sql.SQL("CREATE TABLE {} ({})").format(
                                    quoted_table, column_definitions
                                )
                            )

                            copy_statement = sql.SQL(
                                "COPY {} FROM STDIN WITH (FORMAT csv)"
                            ).format(quoted_table)

                            rows_inserted = 0
                            for chunk in [first_chunk, *chunk_reader]:
                                chunk = chunk.iloc[:, : len(headers)].copy()
                                chunk.columns = headers
                                if chunk.empty:
                                    continue

                                buffer = io.StringIO()
                                chunk.to_csv(buffer, index=False, header=False)
                                buffer.seek(0)
                                cursor.copy_expert(copy_statement, buffer)
                                rows_inserted += len(chunk)
                                print(
                                    f"  {member.filename}: {rows_inserted:,} rows so far",
                                    flush=True,
                                )

                        print(
                            f"Loaded {rows_inserted:,} rows from {member.filename} into {table_name}",
                            flush=True,
                        )
    finally:
        connection.close()


load_csv_files()

print(f"Loaded CSV data from {zip_path.resolve()} into Postgres", flush=True)