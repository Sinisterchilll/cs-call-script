"""
Fetch call records from Tata Tele Smartflo API and push to Postgres.
Supports multiple Tata Tele accounts (inbound + outbound).
Designed to run daily as a GitHub Actions cron job.

Usage:
    python scripts/fetch_calls.py                    # Fetch yesterday's calls
    python scripts/fetch_calls.py --date 2026-03-01  # Fetch specific date
    python scripts/fetch_calls.py --days 3           # Fetch last 3 days
    python scripts/fetch_calls.py --limit 5          # Test with 5 records per account
    python scripts/fetch_calls.py --dry-run          # Preview without DB insert
"""

import os
import sys
import json
import argparse
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta, timezone
import time

# ============================================
# CONFIGURATION (from env vars or defaults)
# ============================================

# Two Tata Tele accounts: inbound + outbound
TATA_TELE_ACCOUNTS = [
    {
        "name": "Inbound",
        "api_key": os.environ.get(
            "TATA_TELE_API_KEY_INBOUND",
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIzNDY3MzkiLCJjciI6dHJ1ZSwiaXNzIjoiaHR0cHM6Ly9jbG91ZHBob25lLnRhdGF0ZWxlc2VydmljZXMuY29tL3Rva2VuL2dlbmVyYXRlIiwiaWF0IjoxNzcxMzEwNTMwLCJleHAiOjIwNzEzMTA1MzAsIm5iZiI6MTc3MTMxMDUzMCwianRpIjoiTUU4UlEwS0RGSHp2b1NpRCJ9.dhmdj-3KwliH5e7_gGIcy-BMwR7m4zNm2Mx_RzzGlr8"
        ),
    },
    {
        "name": "Outbound",
        "api_key": os.environ.get(
            "TATA_TELE_API_KEY_OUTBOUND",
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI2MDEwNzMiLCJjciI6ZmFsc2UsImlzcyI6Imh0dHBzOi8vY2xvdWRwaG9uZS50YXRhdGVsZXNlcnZpY2VzLmNvbS90b2tlbi9nZW5lcmF0ZSIsImlhdCI6MTc3MjQ4MTcxNywiZXhwIjoyMDcyNDgxNzE3LCJuYmYiOjE3NzI0ODE3MTcsImp0aSI6IloxYmRBVU83eFdPYWo0b3EifQ.TXahtN_dvNdVBKkoEQpQcrPqt4njkrK9WTtx5fEHvFA"
        ),
    },
]

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres.lrnxhtplcuogmbtbzfmx:RaSIDQjfdzBzSiYJ@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"
)

API_BASE = "https://api-smartflo.tatateleservices.com"
API_PAGE_SIZE = 100      # Records per API page
RATE_LIMIT_DELAY = 0.5   # Seconds between API requests (avoid rate limits)

TABLE_NAME = "smartflo data"


# ============================================
# TATA TELE API
# ============================================

def fetch_call_records(api_key, date_str, page=1, limit=100):
    """Fetch call records from Smartflo API for a specific date."""
    url = f"{API_BASE}/v1/call/records"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    params = {
        "start_date": date_str,
        "end_date": date_str,
        "page": page,
        "limit": limit,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_records_for_date(api_key, account_name, date_str, max_records=None):
    """Fetch call records for a specific date only.
    
    NOTE: The Tata Tele API ignores start_date/end_date and returns ALL records
    sorted newest-first. We filter by the 'date' field in each record and stop
    paginating once records are older than our target date.
    """
    matched_records = []
    page = 1
    target_date = date_str  # e.g. "2026-03-01"

    while True:
        data = fetch_call_records(api_key, date_str, page=page, limit=API_PAGE_SIZE)
        results = data.get("results", [])

        if not results:
            break

        # Filter: only keep records matching the target date
        page_matched = 0
        oldest_date_on_page = None
        for rec in results:
            rec_date = rec.get("date", "")
            oldest_date_on_page = rec_date
            if rec_date == target_date:
                matched_records.append(rec)
                page_matched += 1

        print(f"    Page {page}: {page_matched} matched (page had {len(results)} records, oldest: {oldest_date_on_page})")

        # Stop conditions:
        # 1. Records are now older than our target date
        if oldest_date_on_page and oldest_date_on_page < target_date:
            print(f"    Reached records older than {target_date}, stopping.")
            break

        # 2. Max records limit
        if max_records and len(matched_records) >= max_records:
            matched_records = matched_records[:max_records]
            break

        page += 1
        time.sleep(RATE_LIMIT_DELAY)  # Rate limit between pages

    return matched_records


# ============================================
# FIELD MAPPING: API response -> DB columns
# ============================================

def format_duration(seconds):
    """Convert seconds (int) to H:MM:SS format like the DB has."""
    if seconds is None:
        return "0:00:00"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}:{m:02d}:{s:02d}"


def format_call_flow(call_flow_list):
    """Convert API call_flow JSON to the text format stored in DB."""
    if not call_flow_list:
        return "N/A"
    parts = []
    for step in call_flow_list:
        step_type = step.get("type", "")
        name = step.get("name", step.get("value", ""))
        time_str = step.get("readableTime", "")
        if step_type == "init":
            parts.append(f"Customer: (Answer)({time_str})")
        elif step_type == "hangup":
            parts.append(f"Hangup: ({time_str})")
        elif name:
            parts.append(f"{step_type.replace('+', ' ')}: {name}({time_str})")
    return ", ".join(parts) if parts else "N/A"


def map_api_to_db(record):
    """Map a single API record to DB row dict matching 'smartflo data' columns."""
    circle_data = record.get("circle", {}) or {}
    call_flow_raw = record.get("call_flow", [])

    # Build IVR / Auto Attendant / Inbound Queue / Time Group from call_flow
    ivr_items = []
    aa_items = []
    queue_items = []
    tg_items = []

    for step in (call_flow_raw or []):
        stype = step.get("type", "")
        sid = step.get("id", "")
        sname = step.get("name", "")

        if "IVR" in stype or "Ivr" in stype:
            entry = {"id": sid, "name": sname}
            if step.get("dtmf"):
                entry["dtmf"] = step["dtmf"]
            ivr_items.append(entry)
        elif "Auto" in stype and "Attendant" in stype:
            aa_items.append({"id": sid, "name": sname})
        elif "Inbound" in stype or "queue" in stype.lower():
            queue_items.append({"id": sid, "name": sname})
        elif "TimeGroup" in stype:
            tg_items.append({"id": sid, "name": sname})

    # Missed agents
    missed = record.get("missed_agents", [])
    missed_str = ", ".join([a.get("name", "") for a in missed]) if missed else "N/A"

    # Agent hangup cause
    ahd = record.get("agent_hangup_data")
    agent_hangup = "N/A"
    if ahd and isinstance(ahd, dict):
        agent_name = ahd.get("name", "")
        agent_status = ahd.get("status", "")
        agent_hangup = f"{agent_name}: {agent_status}" if agent_name else "N/A"
    elif ahd and isinstance(ahd, list) and len(ahd) > 0:
        parts = []
        for a in ahd:
            parts.append(f"{a.get('name', '')}({a.get('number', '')}): {a.get('status', '')}")
        agent_hangup = ", ".join(parts)

    # Dialer details
    dialer = record.get("dialer_call_details") or {}
    campaign_id = str(dialer.get("campaign_id", "N/A")) if dialer else "N/A"
    campaign_name = dialer.get("campaign_name", "N/A") if dialer else "N/A"
    list_id = str(dialer.get("list_id", "N/A")) if dialer else "N/A"
    list_name = dialer.get("list_name", "N/A") if dialer else "N/A"

    # Disposition
    custom_status = record.get("custom_status") or {}
    disp_names = "N/A"
    disp_codes = "N/A"
    if custom_status and isinstance(custom_status, dict):
        disp_names = custom_status.get("name", "N/A")
        disp_codes = custom_status.get("code", "N/A")

    # Build the timestamp
    date_str = record.get("date", "")
    time_str = record.get("time", "")
    timestamp = None
    if date_str and time_str:
        try:
            timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
        except:
            timestamp = None

    # Call sub type / direction
    hint = record.get("call_hint", "")
    direction = (record.get("direction") or "").capitalize()
    call_sub_type = "N/A"
    if hint == "inbound" and direction == "Outbound":
        call_sub_type = "Inbound Call"

    # DTMF from IVR
    ivr_dtmf = ""
    for item in ivr_items:
        if "dtmf" in item:
            ivr_dtmf = f"{item['id']}:{item['dtmf']}:01"

    # Service mapping
    service = record.get("service", "N/A")
    call_solution = "N/A"
    if "Dialer" in (service or ""):
        call_solution = "Dialer"
    elif "click" in (service or "").lower():
        call_solution = "Clicktocall"
    elif "Incoming" in (service or "") or "inbound" in (record.get("direction") or ""):
        call_solution = "Incoming"

    return {
        "Direction": direction,
        "Call Status": (record.get("status") or "").capitalize(),
        "Call ID": Json(record.get("call_id", "")),  # jsonb column
        "Time": timestamp,
        "Customer Number": int(record.get("client_number", "0").replace("+", "")) if record.get("client_number") else None,
        "Customer Name": record.get("contact_details", {}).get("name", "N/A") if isinstance(record.get("contact_details"), dict) else "N/A",
        "Call Solution": call_solution,
        "DID": int(record.get("did_number", "0").replace("+", "")) if record.get("did_number") else None,
        "Agent Number": record.get("agent_number_with_prefix") or record.get("agent_number") or "N/A",
        "Answered By Agent": record.get("agent_name") or "N/A",
        "Hangup Cause": record.get("hangup_cause") or "N/A",
        "Agent Hangup Cause": agent_hangup,
        "Campaign ID": campaign_id,
        "Campaign Name": campaign_name,
        "Call Start Time": "N/A",
        "Call Answered Time": "N/A",
        "Call End Time": "N/A",
        "Conversation Duration": format_duration(record.get("answered_seconds")),
        "Call Duration": format_duration(record.get("call_duration") or record.get("total_call_duration")),
        "Call Flow": format_call_flow(call_flow_raw),
        "Missed By Agent(s)": missed_str,
        "Recording File Name": record.get("aws_call_recording_identifier") or "N/A",
        "Recording": record.get("recording_url") or "N/A",
        "SIP Response Code": "N/A",
        "Reason Key": record.get("reason") or "N/A",
        "Circle": circle_data.get("circle") or "N/A",
        "Operator": circle_data.get("operator") or "N/A",
        "Call Scope": "domestic",
        "Auto Attendant": json.dumps(aa_items) if aa_items else "N/A",
        "Ivr": json.dumps(ivr_items) if ivr_items else "N/A",
        "Department": record.get("department_name") or "N/A",
        "Voicemail": "N/A",
        "Time Group": json.dumps(tg_items) if tg_items else "N/A",
        "Notes": record.get("notes") or "N/A",
        "Agent Ring Time": record.get("agent_ring_time") or "N/A",
        "Lead ID": record.get("lead_id") or "N/A",
        "List ID": list_id,
        "List Name": list_name,
        "Inbound Queue": json.dumps(queue_items) if queue_items else "N/A",
        "Wrap up Duration": "N/A",
        "Hold Duration": "0:00:00",
        "Hold Count": "0",
        "Retry count": "0",
        "SLA Adherence": "N/A",
        "Short Calls": "N/A",
        "Dialer Disposition": "N/A",
        "Dialer Sub Disposition": "N/A",
        "First Call Resolution": "N/A",
        "Ring Time": "N/A",
        "Feedback Rating": "N/A",
        "Feedback Recording Type": "N/A",
        "Listen To": "N/A",
        "Listen Duration": "0",
        "Listen By": "N/A",
        "Whisper To": "N/A",
        "Whisper Duration": "0",
        "Whisper By": "N/A",
        "Barge To": "N/A",
        "Barge Duration": "0",
        "Barge By": "N/A",
        "Disposition Names": disp_names,
        "Disposition Codes": disp_codes,
        "External Reference ID": record.get("ref_id") or "N/A",
        "AMD Status": "N/A",
        "Inbound Duration": format_duration(record.get("call_duration")) if record.get("direction") == "inbound" else "0:00:00",
        "Outbound Duration": format_duration(record.get("answered_seconds")) if record.get("direction") == "outbound" else "0:00:00",
        "Call Sub Type": call_sub_type,
        "Wait Duration": "0:00:00",
        "Answer Duration": "0:00:00",
        "Skill ID": record.get("sid") or "N/A",
        "Skill Name": record.get("sname") or "N/A",
        "DTMF": ivr_dtmf or "N/A",
        "Dial Assist Score (%)": "N/A",
    }


# ============================================
# DATABASE
# ============================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_existing_call_ids(conn, date_str):
    """Get Call IDs already in DB for the given date to avoid duplicates.
    Checks date + next day to handle timezone/midnight crossover."""
    cur = conn.cursor()
    try:
        # Call ID is jsonb — cast to text and strip quotes for comparison
        # Check date AND next day since API records can cross midnight (IST→UTC)
        cur.execute(
            f'SELECT DISTINCT "Call ID"::text FROM "{TABLE_NAME}" '
            f"WHERE \"Time\"::date >= %s AND \"Time\"::date <= (%s::date + interval '1 day')",
            (date_str, date_str)
        )
        return {row[0].strip('"') for row in cur.fetchall()}
    except Exception as e:
        print(f"    Warning: could not check existing IDs: {e}")
        conn.rollback()
        return set()


def insert_call_records(conn, records):
    """Insert call records into the DB. Returns count of inserted."""
    if not records:
        return 0

    columns = list(records[0].keys())
    # Escape % in column names as %% (psycopg2 interprets % as format char)
    col_str = ", ".join([f'"{c.replace("%", "%%")}"' for c in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f'INSERT INTO "{TABLE_NAME}" ({col_str}) VALUES ({placeholders})'

    cur = conn.cursor()
    inserted = 0
    for record in records:
        values = []
        for c in columns:
            v = record[c]
            # Safety: convert any dict/list to JSON string (Postgres text cols can't store Python dicts)
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            values.append(v)
        try:
            cur.execute(insert_sql, values)
            inserted += 1
        except psycopg2.IntegrityError:
            conn.rollback()
            continue
        except Exception as e:
            conn.rollback()
            print(f"    Error inserting {record.get('Call ID')}: {e}")
            continue

    conn.commit()
    return inserted


# ============================================
# MAIN
# ============================================

def process_account(account, dates, conn, max_records=None, dry_run=False):
    """Fetch and insert records for one Tata Tele account."""
    name = account["name"]
    api_key = account["api_key"]
    total_fetched = 0
    total_inserted = 0

    print(f"\n{'─'*50}")
    print(f"  📞 Account: {name}")
    print(f"{'─'*50}")

    for date_str in dates:
        print(f"\n  📅 {date_str}...")

        # Get existing IDs to skip duplicates
        existing_ids = set()
        if conn:
            existing_ids = get_existing_call_ids(conn, date_str)
            print(f"    Already in DB (this date range): {len(existing_ids)}")

        # Fetch from API
        try:
            records = fetch_all_records_for_date(api_key, name, date_str, max_records=max_records)
        except requests.exceptions.HTTPError as e:
            print(f"    ❌ API error: {e}")
            continue
        except Exception as e:
            print(f"    ❌ Error: {e}")
            continue

        print(f"    Fetched from API: {len(records)}")
        total_fetched += len(records)

        # Map and filter duplicates
        db_rows = []
        for rec in records:
            call_id = rec.get("call_id", "")
            if call_id in existing_ids:
                continue
            db_rows.append(map_api_to_db(rec))

        print(f"    New records: {len(db_rows)}")

        if dry_run:
            for row in db_rows[:2]:
                print(f"\n    --- Sample: {row['Call ID']} ---")
                for k in ["Direction", "Call Status", "Time", "Customer Number",
                           "Answered By Agent", "Conversation Duration"]:
                    print(f"      {k}: {row[k]}")
        elif db_rows:
            inserted = insert_call_records(conn, db_rows)
            total_inserted += inserted
            print(f"    ✅ Inserted: {inserted}")

        # Rate limit between date fetches
        time.sleep(RATE_LIMIT_DELAY)

    return total_fetched, total_inserted


def main():
    parser = argparse.ArgumentParser(description="Fetch Tata Tele call records and push to Postgres")
    parser.add_argument("--date", help="Specific date to fetch (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=1, help="Days back to fetch (default: 1 = yesterday)")
    parser.add_argument("--limit", type=int, default=None, help="Max records per account per day (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't insert into DB")
    args = parser.parse_args()

    print("=" * 60)
    print("  TATA TELE → POSTGRES CALL DATA FETCHER")
    print(f"  Accounts: {len(TATA_TELE_ACCOUNTS)} ({', '.join(a['name'] for a in TATA_TELE_ACCOUNTS)})")
    print("=" * 60)

    # Determine dates to fetch
    if args.date:
        dates = [args.date]
    else:
        dates = []
        for i in range(1, args.days + 1):
            IST = timezone(timedelta(hours=5, minutes=30))
            d = (datetime.now(IST) - timedelta(days=i)).strftime("%Y-%m-%d")
            dates.append(d)

    print(f"  Dates: {', '.join(dates)}")

    # Connect to DB
    conn = None
    if not args.dry_run:
        conn = get_db_connection()
        print(f"  ✅ Connected to Postgres")
    else:
        print("  🔍 DRY RUN — will not insert into DB")

    grand_fetched = 0
    grand_inserted = 0

    for account in TATA_TELE_ACCOUNTS:
        fetched, inserted = process_account(
            account, dates, conn,
            max_records=args.limit,
            dry_run=args.dry_run
        )
        grand_fetched += fetched
        grand_inserted += inserted

        # Rate limit between accounts
        time.sleep(1)

    # Summary
    print(f"\n{'='*60}")
    print(f"  ✅ ALL DONE")
    print(f"{'='*60}")
    print(f"  Dates:    {', '.join(dates)}")
    print(f"  Accounts: {len(TATA_TELE_ACCOUNTS)}")
    print(f"  Fetched:  {grand_fetched}")
    print(f"  Inserted: {grand_inserted}")

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
