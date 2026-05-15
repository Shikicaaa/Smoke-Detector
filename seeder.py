import argparse
import asyncio
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

import os, sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

from backend.services.env import DATABASE_URL
from backend.models.database.core import Base
from backend.models.database.sensor_data import SensorData

# csv column - model column
COLUMN_MAP: dict[str, str] = {
    "utc": "time",
    "temperature[c]": "temperature_c",
    "temperature": "temperature_c",
    "humidity[%]": "humidity_percent",
    "humidity": "humidity_percent",
    "tvoc[ppb]": "tvoc_ppb",
    "tvoc": "tvoc_ppb",
    "eco2[ppm]": "eco2_ppm",
    "eco2": "eco2_ppm",
    "co2": "eco2_ppm",
    "raw[h2]": "raw_h2",
    "raw ethanol": "raw_ethanol",
    "pressure[hpa]": "pressure_hpa",
    "pressure": "pressure_hpa",
    "pm1.0": "pm10",
    "pm2.5": "pm25",
    "nc0.5": "nc05",
    "nc05": "nc05",
    "nc1.0": "nc10",
    "nc2.5": "nc25",
    "nc2_5": "nc25",
    "fire alarm": "fire_alarm",
    "alarm": "fire_alarm",
}

BOOL_TRUE = {"true", "1", "yes", "t"}
BATCH_SIZE = 1_000


def parse_row(raw: dict[str, str], header_map: dict[str, str]) -> dict | None:
    record: dict = {}
    for csv_col, value in raw.items():
        model_col = header_map.get(csv_col.strip().lower())
        if model_col is None:
            continue
        value = value.strip()
        if value == "" or value.lower() in ("null", "none", "nan"):
            record[model_col] = None
            continue
        try:
            if model_col == "time":
                try:
                    timestamp = float(value)
                    record[model_col] = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except ValueError:
                    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z",
                                "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                        try:
                            dt = datetime.strptime(value, fmt)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            record[model_col] = dt
                            break
                        except ValueError:
                            continue
                    else:
                        record[model_col] = datetime.fromisoformat(value)
            elif model_col == "fire_alarm":
                record[model_col] = value.lower() in BOOL_TRUE
            elif model_col in ("tvoc_ppb", "eco2_ppm", "raw_h2", "raw_ethanol"):
                record[model_col] = int(float(value))
            else:
                record[model_col] = float(value)
        except (ValueError, TypeError) as exc:
            print(f"Skipping field '{model_col}' value '{value}': {exc}")
            record[model_col] = None

    if "time" not in record or record.get("time") is None:
        return None
    return record


async def seed(csv_path: Path, truncate: bool = False):
    if not DATABASE_URL:
        print("DATABASE_URL is not set. Check your .env file.")
        sys.exit(1)

    engine = create_async_engine(DATABASE_URL, echo=False)
    SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

    print(f"Reading {csv_path} …")
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print("CSV has no header row.")
            sys.exit(1)

        header_map = {
            h.strip().lower(): COLUMN_MAP.get(h.strip().lower())
            for h in reader.fieldnames
            if COLUMN_MAP.get(h.strip().lower())
        }
        print(f"Column mapping: {header_map}")

        rows = list(reader)

    print(f"{len(rows):,} rows found.")

    records: list[dict] = []
    skipped = 0
    for raw in rows:
        parsed = parse_row(raw, header_map)
        if parsed is None:
            skipped += 1
        else:
            records.append(parsed)

    if skipped:
        print(f"Skipped {skipped} rows (missing timestamp).")

    print(f"Inserting {len(records):,} records in batches of {BATCH_SIZE} …")

    async with SessionLocal() as db:
        if truncate:
            await db.execute(SensorData.__table__.delete())
            await db.commit()
            print("Table truncated.")

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            stmt = insert(SensorData).values(batch).on_conflict_do_nothing(
                index_elements=["time"]
            )
            await db.execute(stmt)
            await db.commit()
            print(f"{min(i + BATCH_SIZE, len(records)):,} / {len(records):,}")

    await engine.dispose()
    print("Seeding complete.")


def main():
    parser = argparse.ArgumentParser(description="Seed sensor_data from CSV")
    parser.add_argument("--csv", required=True, type=Path, help="Path to CSV file")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the table before inserting",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"File not found: {args.csv}")
        sys.exit(1)

    asyncio.run(seed(args.csv, truncate=args.truncate))


if __name__ == "__main__":
    main()