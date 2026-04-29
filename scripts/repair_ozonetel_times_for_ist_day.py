"""
Re-set \"Time\" from Ozonetel CallDate + StartTime with IST (see _combine_datetime).

Use for historical rows ingested before IST-aware storage. New fetches already write
correct timestamptz via map_ozonetel_detail_to_db → _combine_datetime.

  python scripts/repair_ozonetel_times_for_ist_day.py --date 2026-04-24 --chunk-hours 6

Project rule + SQL template: .cursor/rules/ozonetel-ist-analytics.mdc,
scripts/ozonetel_logical_calls_metrics.sql
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from fetch_calls import (
    CDR_SOURCE_COLUMN,
    CDR_SOURCE_OZONETEL,
    DATABASE_URL,
    OZONETEL_UCID_COLUMN,
    TABLE_NAME,
    get_db_connection,
)
from fetch_ozonetel_calls import (
    _combine_datetime,
    ozonetel_leg_key,
)
from backfill_ozonetel_missing_for_day import _fetch_day_details


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--chunk-hours", type=int, default=6)
    args = parser.parse_args()
    if not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)

    day = args.date.strip()
    print(f"Fetch API legs IST {day}, repair \"Time\"...", flush=True)
    details = _fetch_day_details(day, args.chunk_hours)
    conn = get_db_connection()
    cur = conn.cursor()
    n = 0
    for d in details:
        lk = ozonetel_leg_key(d)
        if not lk:
            continue
        start_t = d.get("StartTime") or d.get("PickupTime") or "00:00:00"
        ts = _combine_datetime(d.get("CallDate"), start_t)
        if ts is None:
            continue
        cur.execute(
            f'''
            UPDATE "{TABLE_NAME}"
            SET "Time" = %s
            WHERE "{CDR_SOURCE_COLUMN}" = %s
              AND "{OZONETEL_UCID_COLUMN}" = %s
            ''',
            (ts, CDR_SOURCE_OZONETEL, lk),
        )
        n += cur.rowcount
        if n and n % 400 == 0:
            conn.commit()
            print(f"  ... {n} row updates so far", flush=True)
    conn.commit()
    print(f"  Done. UPDATEs applied (sum of rowcount): {n} (API legs: {len(details)})")
    conn.close()


if __name__ == "__main__":
    main()
