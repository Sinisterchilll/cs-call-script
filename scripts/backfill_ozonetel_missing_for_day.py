"""
Sync one IST calendar day with Ozonetel fetchCDRDetails (all campaigns):

  • \"Call ID\" = parent CallID from API (logical call), never UCID.
  • \"Ozonetel UCID\" = per-leg id (dedupe / 1:1 with API detail rows).
  • Goal: one DB row per API leg for that day — no missing UCIDs after a successful run.

Pipeline:
  1) Batch UPDATE rows that already have \"Ozonetel UCID\" SET \"Call ID\" = parent from API.
  2) Tight PATCH (±15s) on NULL-UCID rows (campaign, customer, status, agent).
  3) Loose PATCH (same fields, nearest \"Time\" within ±15 minutes, one DB row per API leg).
  4) INSERT any API legs still absent.

Requires DATABASE_URL, OZONETEL_*. Applies \"Ozonetel UCID\" column if missing.

Usage:
  python scripts/backfill_ozonetel_missing_for_day.py --date 2026-04-24
  python scripts/backfill_ozonetel_missing_for_day.py --date 2026-04-24 --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import timedelta
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

import psycopg2

from fetch_calls import (
    CDR_SOURCE_COLUMN,
    CDR_SOURCE_OZONETEL,
    DATABASE_URL,
    OZONETEL_UCID_COLUMN,
    TABLE_NAME,
    get_db_connection,
    get_existing_ozonetel_ucids,
    insert_call_records,
)
from fetch_ozonetel_calls import (
    CDR_REQUEST_INTERVAL_SEC,
    OZONETEL_API_KEY,
    OZONETEL_DOMAIN,
    OZONETEL_USERNAME,
    fetch_cdr_details,
    generate_token,
    map_ozonetel_detail_to_db,
    ozonetel_leg_key,
)


def _ensure_ucid_column(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        f'ALTER TABLE "{TABLE_NAME}" '
        f'ADD COLUMN IF NOT EXISTS "{OZONETEL_UCID_COLUMN}" text'
    )
    conn.commit()


def _ist_subwindows(day: str, chunk_hours: int) -> list[tuple[str, str]]:
    if chunk_hours <= 0 or chunk_hours > 24:
        return [(f"{day} 00:00:00", f"{day} 23:59:59")]
    windows: list[tuple[str, str]] = []
    start_h = 0
    while start_h < 24:
        end_h = min(start_h + chunk_hours, 24) - 1
        windows.append((f"{day} {start_h:02d}:00:00", f"{day} {end_h:02d}:59:59"))
        start_h += chunk_hours
    return windows


def _fetch_day_details(day: str, chunk_hours: int) -> list[dict]:
    token = generate_token(OZONETEL_DOMAIN, OZONETEL_API_KEY, OZONETEL_USERNAME)
    windows = _ist_subwindows(day, chunk_hours)
    merged: list[dict] = []
    seen_uc: set[str] = set()
    for i, (fr, to) in enumerate(windows):
        if i:
            time.sleep(CDR_REQUEST_INTERVAL_SEC)
        print(f"  fetchCDRDetails {i + 1}/{len(windows)}: {fr} → {to}", flush=True)
        part = fetch_cdr_details(
            OZONETEL_DOMAIN,
            token,
            OZONETEL_API_KEY,
            OZONETEL_USERNAME,
            fr,
            to,
            campaign_name=None,
        )
        for d in part:
            lk = ozonetel_leg_key(d)
            if lk and lk not in seen_uc:
                seen_uc.add(lk)
                merged.append(d)
    return merged


def _parent_from_detail(detail: dict, ucid: str) -> str:
    p = str(detail.get("CallID") or detail.get("callId") or "").strip()
    return p or ucid


def _batch_set_call_id_for_known_ucids(
    conn, day: str, ucid_to_parent: dict[str, str]
) -> int:
    """Force \"Call ID\" = parent for every row whose Ozonetel UCID is in the API map."""
    if not ucid_to_parent:
        return 0
    cur = conn.cursor()
    items = list(ucid_to_parent.items())
    batch = 400
    total = 0
    for i in range(0, len(items), batch):
        chunk = items[i : i + batch]
        ucids = [p[0] for p in chunk]
        parents = [p[1] for p in chunk]
        cur.execute(
            f'''
            UPDATE "{TABLE_NAME}" AS s
            SET "Call ID" = to_jsonb(m.parent::text)
            FROM unnest(%s::text[], %s::text[]) AS m(ucid, parent)
            WHERE s."{CDR_SOURCE_COLUMN}" = %s
              AND (s."Time" AT TIME ZONE 'Asia/Kolkata')::date = %s::date
              AND s."{OZONETEL_UCID_COLUMN}" = m.ucid
            ''',
            (ucids, parents, CDR_SOURCE_OZONETEL, day),
        )
        total += cur.rowcount
    conn.commit()
    return total


def _try_patch_null_ucid_row(cur, day: str, row: dict, detail: dict, window: timedelta) -> bool:
    ucid = row[OZONETEL_UCID_COLUMN]
    parent = _parent_from_detail(detail, ucid)
    camp = row["Campaign Name"]
    cust = row["Customer Number"]
    st = row["Call Status"]
    agent = row["Answered By Agent"]
    ts = row["Time"]
    if ts is None:
        return False
    lo = ts - window
    hi = ts + window
    cur.execute(
        f'''
        UPDATE "{TABLE_NAME}" AS s
        SET "{OZONETEL_UCID_COLUMN}" = %s,
            "Call ID" = to_jsonb(%s::text)
        WHERE s.ctid = (
            SELECT t.ctid FROM "{TABLE_NAME}" t
            WHERE t."{CDR_SOURCE_COLUMN}" = %s
              AND (t."Time" AT TIME ZONE 'Asia/Kolkata')::date = %s::date
              AND (t."{OZONETEL_UCID_COLUMN}" IS NULL OR TRIM(t."{OZONETEL_UCID_COLUMN}") = '')
              AND t."Campaign Name" IS NOT DISTINCT FROM %s
              AND t."Customer Number" IS NOT DISTINCT FROM %s
              AND t."Call Status" IS NOT DISTINCT FROM %s
              AND t."Answered By Agent" IS NOT DISTINCT FROM %s
              AND t."Time" BETWEEN %s AND %s
            ORDER BY t."Time"
            LIMIT 1
        )
        ''',
        (
            ucid,
            parent,
            CDR_SOURCE_OZONETEL,
            day,
            camp,
            cust,
            st,
            agent,
            lo,
            hi,
        ),
    )
    return cur.rowcount > 0


def _try_patch_loose(
    cur,
    day: str,
    row: dict,
    detail: dict,
    claimed_ctids: set[str],
    max_abs_sec: float,
) -> bool:
    """Pick nearest-in-time NULL-UCID row (same campaign, customer, status, agent)."""
    ucid = row[OZONETEL_UCID_COLUMN]
    parent = _parent_from_detail(detail, ucid)
    camp = row["Campaign Name"]
    cust = row["Customer Number"]
    st = row["Call Status"]
    agent = row["Answered By Agent"]
    ts = row["Time"]
    if ts is None:
        return False

    cur.execute(
        f'''
        SELECT ctid::text, "Time"
        FROM "{TABLE_NAME}"
        WHERE "{CDR_SOURCE_COLUMN}" = %s
          AND ("Time" AT TIME ZONE 'Asia/Kolkata')::date = %s::date
          AND ("{OZONETEL_UCID_COLUMN}" IS NULL OR TRIM("{OZONETEL_UCID_COLUMN}") = '')
          AND "Campaign Name" IS NOT DISTINCT FROM %s
          AND "Customer Number" IS NOT DISTINCT FROM %s
          AND "Call Status" IS NOT DISTINCT FROM %s
          AND "Answered By Agent" IS NOT DISTINCT FROM %s
        ''',
        (CDR_SOURCE_OZONETEL, day, camp, cust, st, agent),
    )
    best_ctid: str | None = None
    best_delta = max_abs_sec + 1.0
    for ctid_s, t in cur.fetchall():
        if ctid_s in claimed_ctids or t is None:
            continue
        delta = abs((t - ts).total_seconds())
        if delta < best_delta:
            best_delta = delta
            best_ctid = ctid_s
    if best_ctid is None or best_delta > max_abs_sec:
        return False

    cur.execute(
        f'''
        UPDATE "{TABLE_NAME}"
        SET "{OZONETEL_UCID_COLUMN}" = %s,
            "Call ID" = to_jsonb(%s::text)
        WHERE ctid::text = %s
        ''',
        (ucid, parent, best_ctid),
    )
    if cur.rowcount:
        claimed_ctids.add(best_ctid)
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Ozonetel: Call ID = parent CallID, Ozonetel UCID = leg id, full API coverage"
    )
    parser.add_argument("--date", required=True, help="IST calendar day YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=6,
        help="fetchCDRDetails chunk size (hours). 0 = single 00:00–23:59 request.",
    )
    args = parser.parse_args()
    day = args.date.strip()
    if not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)
    if not OZONETEL_API_KEY or not OZONETEL_USERNAME:
        print("Set OZONETEL_API_KEY and OZONETEL_USERNAME.", file=sys.stderr)
        sys.exit(1)

    ch = args.chunk_hours
    print(f"Fetch all campaigns, IST {day} (chunk_hours={ch})...", flush=True)
    details = _fetch_day_details(day, ch)
    print(f"  Unique API legs: {len(details)}", flush=True)

    ucid_to_parent: dict[str, str] = {}
    for d in details:
        u = ozonetel_leg_key(d)
        if u:
            ucid_to_parent[u] = _parent_from_detail(d, u)

    conn = get_db_connection()
    try:
        _ensure_ucid_column(conn)

        if args.dry_run:
            existing = set(get_existing_ozonetel_ucids(conn, day))
            missing = [u for u in ucid_to_parent if u not in existing]
            print(f"  [dry-run] API legs not yet in DB (by {OZONETEL_UCID_COLUMN}): {len(missing)}")
            return

        n_align = _batch_set_call_id_for_known_ucids(conn, day, ucid_to_parent)
        print(f"  Phase 1 — set \"Call ID\" from API parent for known UCIDs: {n_align} row(s) touched")

        existing = set(get_existing_ozonetel_ucids(conn, day))
        patched_tight = 0
        pending_inserts: list[dict] = []
        cur = conn.cursor()
        pending_commit = 0

        for d in details:
            lk = ozonetel_leg_key(d)
            if not lk or lk in existing:
                continue
            row = map_ozonetel_detail_to_db(d)
            if _try_patch_null_ucid_row(cur, day, row, d, timedelta(seconds=15)):
                patched_tight += 1
                existing.add(lk)
                pending_commit += 1
                if pending_commit >= 80:
                    conn.commit()
                    pending_commit = 0
                    cur = conn.cursor()
            else:
                pending_inserts.append(row)

        if pending_commit:
            conn.commit()
        cur = conn.cursor()

        existing = set(get_existing_ozonetel_ucids(conn, day))
        claimed: set[str] = set()
        patched_loose = 0
        still_insert: list[dict] = []
        for row in pending_inserts:
            d = None
            lk = row[OZONETEL_UCID_COLUMN]
            for x in details:
                if ozonetel_leg_key(x) == lk:
                    d = x
                    break
            if d is None or lk in existing:
                continue
            if _try_patch_loose(cur, day, row, d, claimed, max_abs_sec=900.0):
                conn.commit()
                patched_loose += 1
                existing.add(lk)
                cur = conn.cursor()
            else:
                still_insert.append(row)

        n_ins = insert_call_records(conn, still_insert) if still_insert else 0

        n_final = _batch_set_call_id_for_known_ucids(conn, day, ucid_to_parent)
        after = set(get_existing_ozonetel_ucids(conn, day))
        missing_final = [u for u in ucid_to_parent if u not in after]

        print(f"  Phase 2 — tight PATCH (±15s): {patched_tight}")
        print(f"  Phase 3 — loose PATCH (≤15m nearest): {patched_loose}")
        print(f"  Phase 4 — INSERT new legs: {n_ins} (attempted {len(still_insert)})")
        print(f"  Phase 5 — re-align \"Call ID\" from API: {n_final} row(s) touched")
        print(f"  Distinct API UCIDs: {len(ucid_to_parent)} | in DB after run: {len(after)}")
        if missing_final:
            print(f"  ⚠️  Still missing {len(missing_final)} UCID(s) (sample): {missing_final[:8]}")
        else:
            print("  ✓ Every API leg has a matching \"Ozonetel UCID\" in DB for this IST day.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
