"""
Fetch fetchCDRDetails for one full IST day (all campaigns), merge chunked windows,
then INSERT or UPDATE every leg (\"Ozonetel UCID\"). Same mapping as single-campaign sync:
  \"Call ID\" = parent CallID, \"Ozonetel UCID\" = UCID, \"Time\" = API CallDate + StartTime as IST (_combine_datetime).

Use this when you want the whole day in one shot (e.g. same as Apr 24 full-day pipeline).

Usage:
  python scripts/sync_ozonetel_full_ist_day.py --date 2026-04-25
  python scripts/sync_ozonetel_full_ist_day.py --date 2026-04-25 --chunk-hours 6
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from backfill_ozonetel_missing_for_day import _ensure_ucid_column, _fetch_day_details
from fetch_calls import DATABASE_URL, get_db_connection, upsert_ozonetel_call_records
from fetch_ozonetel_calls import (
    OZONETEL_API_KEY,
    OZONETEL_DOMAIN,
    OZONETEL_USERNAME,
    map_ozonetel_detail_to_db,
    ozonetel_leg_key,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="IST calendar day YYYY-MM-DD")
    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=6,
        help="Chunk size for fetchCDRDetails (0 = single 00:00–23:59). Default 6.",
    )
    args = parser.parse_args()

    if not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)
    if not OZONETEL_API_KEY or not OZONETEL_USERNAME:
        print("Set OZONETEL_API_KEY and OZONETEL_USERNAME.", file=sys.stderr)
        sys.exit(1)

    day = args.date.strip()
    print(f"sync_ozonetel_full_ist_day: {day} (chunk_hours={args.chunk_hours})", flush=True)
    details = _fetch_day_details(day, args.chunk_hours)
    rows = []
    for d in details:
        if not ozonetel_leg_key(d):
            continue
        rows.append(map_ozonetel_detail_to_db(d))

    print(f"  Unique API legs: {len(details)} | mapped rows: {len(rows)}", flush=True)

    conn = get_db_connection()
    try:
        _ensure_ucid_column(conn)
        ins, upd = upsert_ozonetel_call_records(conn, rows)
        print(f"  Inserted: {ins} | Updated: {upd}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
