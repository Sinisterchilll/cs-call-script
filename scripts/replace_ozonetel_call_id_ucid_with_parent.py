"""
One-off / repair: for a single IST calendar day, set \"Call ID\" to Ozonetel's
parent CallID (logical call) instead of UCID, for all rows where CDR Source = ozonetel.

Rows are matched by current \"Call ID\" value (UCID as stored) + date + source.

Requires OZONETEL_* env vars and DATABASE_URL (same as fetch_ozonetel_calls.py).

Usage:
  python scripts/replace_ozonetel_call_id_ucid_with_parent.py --date 2026-04-23 --dry-run
  python scripts/replace_ozonetel_call_id_ucid_with_parent.py --date 2026-04-23
  python scripts/replace_ozonetel_call_id_ucid_with_parent.py --date 2026-04-24 --chunk-hours 6
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from fetch_calls import (
    CDR_SOURCE_COLUMN,
    CDR_SOURCE_OZONETEL,
    DATABASE_URL,
    TABLE_NAME,
    get_db_connection,
)
from fetch_ozonetel_calls import (
    CDR_REQUEST_INTERVAL_SEC,
    OZONETEL_API_KEY,
    OZONETEL_DOMAIN,
    OZONETEL_USERNAME,
    fetch_cdr_details,
    generate_token,
)


def _ucid_to_parent_map(details: list[dict]) -> dict[str, str]:
    """UCID (detail leg id) -> parent CallID from Ozonetel."""
    out: dict[str, str] = {}
    for d in details:
        ucid = str(d.get("UCID") or d.get("ucid") or "").strip()
        parent = str(d.get("CallID") or d.get("callId") or "").strip()
        if not ucid or not parent:
            continue
        out[ucid] = parent
    return out


def _ist_subwindows(day: str, chunk_hours: int) -> list[tuple[str, str]]:
    """Non-overlapping [from, to] strings for one IST calendar day."""
    if chunk_hours <= 0 or chunk_hours > 24:
        return [(f"{day} 00:00:00", f"{day} 23:59:59")]
    windows: list[tuple[str, str]] = []
    start_h = 0
    while start_h < 24:
        end_h = min(start_h + chunk_hours, 24) - 1
        fr = f"{day} {start_h:02d}:00:00"
        to = f"{day} {end_h:02d}:59:59"
        windows.append((fr, to))
        start_h += chunk_hours
    return windows


def _merge_ucid_maps(
    acc: dict[str, str], chunk: dict[str, str]
) -> int:
    """Merge chunk into acc. Returns number of UCID keys with conflicting parent."""
    conflicts = 0
    for ucid, parent in chunk.items():
        if ucid in acc and acc[ucid] != parent:
            conflicts += 1
            continue
        if ucid not in acc:
            acc[ucid] = parent
    return conflicts


def _fetch_ucid_map_for_day(
    day: str, chunk_hours: int
) -> tuple[dict[str, str], int, int, int]:
    """
    Returns (merged_ucid_to_parent, total_detail_rows_fetched, api_windows, conflict_count).
    Only \"Call ID\" / UCID mapping uses this; no other table columns are read from the API.
    """
    token = generate_token(OZONETEL_DOMAIN, OZONETEL_API_KEY, OZONETEL_USERNAME)
    windows = _ist_subwindows(day, chunk_hours)
    merged: dict[str, str] = {}
    total_rows = 0
    total_conflicts = 0
    for i, (fr, to) in enumerate(windows):
        if i:
            time.sleep(CDR_REQUEST_INTERVAL_SEC)
        print(f"    fetchCDRDetails window {i + 1}/{len(windows)}: {fr} → {to}", flush=True)
        details = fetch_cdr_details(
            OZONETEL_DOMAIN,
            token,
            OZONETEL_API_KEY,
            OZONETEL_USERNAME,
            fr,
            to,
            campaign_name=None,
        )
        total_rows += len(details)
        part = _ucid_to_parent_map(details)
        total_conflicts += _merge_ucid_maps(merged, part)
    return merged, total_rows, len(windows), total_conflicts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replace Ozonetel Call ID (jsonb) from UCID to parent CallID for one IST day"
    )
    parser.add_argument("--date", required=True, help="IST calendar day YYYY-MM-DD")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch API + show counts; no UPDATE",
    )
    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=6,
        metavar="N",
        help=(
            "Split the IST day into N-hour slices and merge fetchCDRDetails results "
            "(better coverage if a single full-day response truncates). Use 0 for one "
            "request 00:00–23:59 only. Default: 6."
        ),
    )
    args = parser.parse_args()
    day = args.date.strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        print("Invalid --date; use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    if not OZONETEL_API_KEY or not OZONETEL_USERNAME:
        print("Set OZONETEL_API_KEY and OZONETEL_USERNAME.", file=sys.stderr)
        sys.exit(1)
    if not args.dry_run and not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)

    ch = args.chunk_hours
    print(
        f"Token + fetchCDRDetails (IST {day}, all campaigns, chunk_hours={ch})...",
        flush=True,
    )
    ucid_map, api_row_total, n_win, n_conf = _fetch_ucid_map_for_day(day, ch)
    print(
        f"  API detail rows (sum of windows): {api_row_total}  |  windows: {n_win}  "
        f"|  merged UCID→CallID pairs: {len(ucid_map)}",
        flush=True,
    )
    if n_conf:
        print(
            f"  ⚠️  UCID parent conflicts across windows (kept first parent): {n_conf}",
            flush=True,
        )

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f'SELECT TRIM(BOTH \'\"\' FROM "Call ID"::text) AS ucid, COUNT(*) '
            f'FROM "{TABLE_NAME}" '
            f'WHERE "{CDR_SOURCE_COLUMN}" = %s '
            f'AND ("Time" AT TIME ZONE \'Asia/Kolkata\')::date = %s::date '
            f"GROUP BY 1",
            (CDR_SOURCE_OZONETEL, day),
        )
        db_counts: dict[str, int] = {row[0]: row[1] for row in cur.fetchall()}
        n_db = sum(db_counts.values())
        matched = sum(db_counts[u] for u in ucid_map if u in db_counts)
        api_only = len(set(ucid_map) - set(db_counts))
        db_only = len(set(db_counts) - set(ucid_map))

        print(f"  DB Ozonetel rows on {day} (IST): {n_db}")
        print(f"  Distinct UCIDs in DB that day: {len(db_counts)}")
        print(f"  Rows that would match + update: {matched}")
        if api_only:
            print(f"  UCIDs in API map but not in DB: {api_only} (no local row to update)")
        if db_only:
            print(f"  Distinct UCIDs in DB not returned in API map: {db_only} (left unchanged)")

        if args.dry_run:
            return

        update_sql = (
            f'UPDATE "{TABLE_NAME}" AS s '
            f'SET "Call ID" = to_jsonb(m.parent::text) '
            f"FROM unnest(%s::text[], %s::text[]) AS m(ucid, parent) "
            f'WHERE s."{CDR_SOURCE_COLUMN}" = %s '
            f"AND (s.\"Time\" AT TIME ZONE 'Asia/Kolkata')::date = %s::date "
            f'AND TRIM(BOTH \'\"\' FROM s."Call ID"::text) = m.ucid'
        )

        items = list(ucid_map.items())
        batch = 500
        total = 0
        for i in range(0, len(items), batch):
            chunk = items[i : i + batch]
            ucids = [p[0] for p in chunk]
            parents = [p[1] for p in chunk]
            cur.execute(
                update_sql,
                (ucids, parents, CDR_SOURCE_OZONETEL, day),
            )
            total += cur.rowcount
        conn.commit()
        print(f"  Batches: {(len(items) + batch - 1) // batch}  |  total rows updated: {total}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
