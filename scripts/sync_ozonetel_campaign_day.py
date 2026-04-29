"""
Fetch fetchCDRDetails for one IST calendar day and one campaign, then INSERT or UPDATE
every leg in Postgres (match on \"Ozonetel UCID\"). No rows skipped: same row count as API.

\"Call ID\" = parent CallID from API; \"Ozonetel UCID\" = UCID; \"Time\" = API CallDate + StartTime as IST (see _combine_datetime in fetch_ozonetel_calls.py).

Usage:
  python scripts/sync_ozonetel_campaign_day.py --date 2026-04-24 --campaign Customer_support
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from fetch_calls import DATABASE_URL, get_db_connection, upsert_ozonetel_call_records
from fetch_ozonetel_calls import (
    OZONETEL_API_KEY,
    OZONETEL_DOMAIN,
    OZONETEL_USERNAME,
    fetch_cdr_details,
    generate_token,
    map_ozonetel_detail_to_db,
    ozonetel_leg_key,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="IST day YYYY-MM-DD (same as API fromDate/toDate)")
    parser.add_argument("--campaign", required=True, help="Ozonetel campaignName, e.g. Customer_support")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)
    if not OZONETEL_API_KEY or not OZONETEL_USERNAME:
        print("Set OZONETEL_API_KEY and OZONETEL_USERNAME.", file=sys.stderr)
        sys.exit(1)

    day = args.date.strip()
    camp = args.campaign.strip()
    from_dt = f"{day} 00:00:00"
    to_dt = f"{day} 23:59:59"

    print(f"sync_ozonetel_campaign_day: {camp!r} | {from_dt} → {to_dt}", flush=True)
    token = generate_token(OZONETEL_DOMAIN, OZONETEL_API_KEY, OZONETEL_USERNAME)
    details = fetch_cdr_details(
        OZONETEL_DOMAIN,
        token,
        OZONETEL_API_KEY,
        OZONETEL_USERNAME,
        from_dt,
        to_dt,
        campaign_name=camp,
    )
    rows = []
    for d in details:
        if not ozonetel_leg_key(d):
            continue
        rows.append(map_ozonetel_detail_to_db(d))

    print(f"  API legs: {len(details)} | mapped rows: {len(rows)}", flush=True)

    conn = get_db_connection()
    try:
        ins, upd = upsert_ozonetel_call_records(conn, rows)
        print(f"  Inserted: {ins} | Updated: {upd}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
