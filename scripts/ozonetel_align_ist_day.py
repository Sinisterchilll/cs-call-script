"""
Align one IST calendar day with Ozonetel fetchCDRDetails (all campaigns).

**Order (same as we used for fixing historical days)** — do not skip ahead:

**Phase A — UCID in its own column**  
  Legacy rows often stored only the per-leg id inside jsonb ``\"Call ID\"``, with
  ``\"Ozonetel UCID\"`` empty. We copy that leg id into ``\"Ozonetel UCID\"`` so the
  row can be keyed for upsert (only UCIDs returned for this IST day in the API).

**Phase B — Real logical Call ID + IST Time**  
  ``map_ozonetel_detail_to_db`` then ``upsert_ozonetel_call_records`` sets in one shot:
  - ``\"Call ID\"`` = Ozonetel **parent** ``CallID`` / ``callId`` (logical call)
  - ``\"Ozonetel UCID\"`` = per-leg UCID
  - ``\"Time\"`` = ``_combine_datetime(CallDate, StartTime)`` (**IST** correct instant)

New ingests already do B from the API; this script fixes old rows and brings the day in sync.

  python scripts/ozonetel_align_ist_day.py --date 2026-04-26
  python scripts/ozonetel_align_ist_day.py --date 2026-04-28 --chunk-hours 6
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from backfill_ozonetel_missing_for_day import _ensure_ucid_column, _fetch_day_details
from fetch_calls import (
    CDR_SOURCE_COLUMN,
    CDR_SOURCE_OZONETEL,
    DATABASE_URL,
    OZONETEL_UCID_COLUMN,
    TABLE_NAME,
    get_db_connection,
    upsert_ozonetel_call_records,
)
from fetch_ozonetel_calls import (
    OZONETEL_API_KEY,
    OZONETEL_DOMAIN,
    OZONETEL_USERNAME,
    map_ozonetel_detail_to_db,
    ozonetel_leg_key,
)


def phase_hydrate_ucid_column(conn, day: str, api_ucids: set[str]) -> int:
    """
    Phase A: copy per-leg UCID into \"Ozonetel UCID\" when that column was empty
    and jsonb \"Call ID\" still held the UCID string (legacy layout).
    """
    if not api_ucids:
        return 0
    cur = conn.cursor()
    items = list(api_ucids)
    total = 0
    batch = 400
    for i in range(0, len(items), batch):
        chunk = items[i : i + batch]
        cur.execute(
            f'''
            UPDATE "{TABLE_NAME}" AS s
            SET "{OZONETEL_UCID_COLUMN}" = m.ucid
            FROM unnest(%s::text[]) AS m(ucid)
            WHERE s."{CDR_SOURCE_COLUMN}" = %s
              AND (s."Time" AT TIME ZONE 'Asia/Kolkata')::date = %s::date
              AND (s."{OZONETEL_UCID_COLUMN}" IS NULL OR TRIM(s."{OZONETEL_UCID_COLUMN}") = '')
              AND TRIM(BOTH '"' FROM s."Call ID"::text) = m.ucid
            ''',
            (chunk, CDR_SOURCE_OZONETEL, day),
        )
        total += cur.rowcount
    conn.commit()
    return total


def phase_upsert_parent_call_id_and_ist_time(conn, rows: list[dict]) -> tuple[int, int]:
    """
    Phase B: for every API leg, set parent logical Call ID, UCID column, and IST Time
    via the same mapper used for live ingest (see fetch_ozonetel_calls.map_ozonetel_detail_to_db).
    """
    return upsert_ozonetel_call_records(conn, rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ozonetel day align: UCID column → parent Call ID + IST Time (see module docstring)"
    )
    parser.add_argument("--date", required=True, help="IST calendar day YYYY-MM-DD")
    parser.add_argument("--chunk-hours", type=int, default=6)
    args = parser.parse_args()

    if not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)
    if not OZONETEL_API_KEY or not OZONETEL_USERNAME:
        print("Set OZONETEL_API_KEY and OZONETEL_USERNAME.", file=sys.stderr)
        sys.exit(1)

    day = args.date.strip()
    print(f"ozonetel_align_ist_day: {day} (chunk_hours={args.chunk_hours})", flush=True)
    details = _fetch_day_details(day, args.chunk_hours)
    api_ucids = {k for d in details if (k := ozonetel_leg_key(d))}
    print(f"  API legs: {len(details)} | distinct UCID: {len(api_ucids)}", flush=True)

    rows = []
    for d in details:
        lk = ozonetel_leg_key(d)
        if lk:
            rows.append(map_ozonetel_detail_to_db(d))

    conn = get_db_connection()
    try:
        _ensure_ucid_column(conn)
        print("  Phase A — \"Ozonetel UCID\" column (from legacy jsonb Call ID where empty)", flush=True)
        n_h = phase_hydrate_ucid_column(conn, day, api_ucids)
        print(f"    … updated: {n_h} row(s)", flush=True)
        print("  Phase B — parent \"Call ID\" + IST \"Time\" + full row (upsert per UCID)", flush=True)
        ins, upd = phase_upsert_parent_call_id_and_ist_time(conn, rows)
        print(f"    … inserted: {ins} | updated: {upd}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
