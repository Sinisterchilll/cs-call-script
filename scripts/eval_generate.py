"""
Weekly Eval: Generate individual Google Sheets for each team member.

Picks 10 random analyzed calls, creates one sheet per evaluator with:
- call #, recording link, transcript, category dropdown, resolved dropdown
- AI results are hidden (blind evaluation)

Posts sheet links to Slack.

Usage:
    python scripts/eval_generate.py              # Generate sheets & post to Slack
    python scripts/eval_generate.py --dry-run    # Preview selected calls, no sheets/slack
    python scripts/eval_generate.py --test-slack  # Just send a test message to Slack
"""

import os
import sys
import json
import argparse
import psycopg2
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ============================================
# CONFIG
# ============================================

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
SLACK_BOT_TOKEN = (os.environ.get("SLACK_BOT_TOKEN") or "").strip()
SLACK_CHANNEL_ID = (os.environ.get("SLACK_CHANNEL_ID") or "").strip()

GOOGLE_CREDS_FILE = os.environ.get(
    "GOOGLE_CREDENTIALS_FILE",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "google-credentials.json")
)

# Hardcoded spreadsheet IDs (created by the user to bypass 0-byte Service Account quota)
EVAL_SHEETS = {
    "Prudhvi": os.environ.get("SHEET_ID_PRUDHVI", "1ViguRl61iO6eiN1tyHRkHxXitqf8Iglf452s4FFceB4"),
    "Aum": os.environ.get("SHEET_ID_AUM", "1hn5-C5wW8b-O_z7zNFeD-vTDhvyfRMU0rxxwQ7m_LaY"),
    "Ayush": os.environ.get("SHEET_ID_AYUSH", "1UNjafjwQyfNoOATsfjG4q-N2udOXaa4dhRepaPJat8w"),
    "Swarna": os.environ.get("SHEET_ID_SWARNA", "1Mi4wEc9Pxd7LkxOlcUeHJhTMrtsHHgkizoNTwrmewyE"),
    "Uditi": os.environ.get("SHEET_ID_UDITI", "168-BRsA0s1LWtpKz_7loocDvNpFLpoHJdJb4ALpeeBI"),
}

SCORE_SHEET_ID = os.environ.get("SHEET_ID_SCORES", "1DR3Ud-Pqu12T_QVU5VECa75w6zw9OVWWQMtPQPazIoE")

NUM_CALLS = 10  # Calls per eval round

TABLE_CALLS = "smartflo data"
TABLE_TRANSCRIPTS = "call_transcripts"
TABLE_ANALYSIS = "call analysis"

# 30 categories (same as analyze_calls.py)
CATEGORIES = [
    "refund_issue", "recharge_issue", "payment_failure", "billing_dispute", "penalty_or_fine",
    "kyc_document_issue", "kyc_pending", "kyc_guidance", "otp_or_login_issue",
    "plan_pricing_inquiry", "onboarding_or_deposit_fee", "offer_or_promotion", "outbound_offer_call",
    "bike_wont_start", "bike_stopping_mid_ride", "battery_swap_issue", "battery_range_issue",
    "vehicle_repair_or_damage", "vehicle_missing",
    "hub_location_inquiry", "vehicle_availability", "vehicle_pickup_or_return", "hub_service_delay",
    "app_error_or_block", "app_data_mismatch", "referral_issue",
    "no_response_or_disconnected", "general_inquiry", "language_barrier", "service_complaint", "police", "escalation"
]


# ============================================
# DATABASE
# ============================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_next_round_number(conn):
    """Get next eval round number."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(round_number) FROM eval_rounds")
    row = cur.fetchone()
    return (row[0] or 0) + 1


def get_used_call_ids(conn):
    """Get call IDs already used in previous eval rounds."""
    cur = conn.cursor()
    cur.execute("SELECT UNNEST(call_ids) FROM eval_rounds")
    return {row[0] for row in cur.fetchall()}


def pick_eval_calls(conn, count=10):
    """Pick random answered calls with transcripts + AI analysis, excluding already-used calls."""
    used = get_used_call_ids(conn)

    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            ct.call_id,
            ct.transcript,
            sd."Recording",
            ca.issue_category,
            ca.resolved,
            ca.summary
        FROM {TABLE_TRANSCRIPTS} ct
        JOIN "{TABLE_ANALYSIS}" ca ON ca.activity_id = ct.call_id
        JOIN "{TABLE_CALLS}" sd ON TRIM(BOTH '"' FROM sd."Call ID"::text) = ct.call_id
        WHERE ct.transcript IS NOT NULL
          AND LENGTH(ct.transcript) > 50
          AND ca.issue_category IS NOT NULL
          AND sd."Time" >= '2026-03-01'
        ORDER BY RANDOM()
        LIMIT %s
    """, (count + len(used),))  # Fetch extra to filter used ones

    rows = cur.fetchall()
    selected = []
    for row in rows:
        call_id = row[0]
        if call_id not in used and len(selected) < count:
            selected.append({
                "call_id": call_id,
                "transcript": row[1],
                "recording_url": row[2],
                "ai_category": row[3],
                "ai_resolved": row[4],
                "ai_summary": row[5],
            })

    return selected


def save_eval_round(conn, round_number, call_ids, sheet_ids, score_sheet_id):
    """Save the eval round to DB."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO eval_rounds (round_number, call_ids, sheet_ids, score_sheet_id)
        VALUES (%s, %s, %s, %s)
    """, (round_number, call_ids, json.dumps(sheet_ids), score_sheet_id))
    conn.commit()


def ensure_eval_table(conn):
    """Create eval_rounds table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eval_rounds (
            id SERIAL PRIMARY KEY,
            round_number INT NOT NULL,
            call_ids TEXT[] NOT NULL,
            sheet_ids JSONB NOT NULL,
            score_sheet_id TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            scored_at TIMESTAMP
        )
    """)
    conn.commit()


# ============================================
# GOOGLE SHEETS
# ============================================

def get_sheets_service():
    """Build authenticated Google Sheets + Drive service."""
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


def populate_eval_sheet(sheets_service, sheet_id, evaluator_name, round_number, calls):
    """Populate an existing Google Sheet for one evaluator with the 10 calls."""
    if not sheet_id:
        raise ValueError(f"No sheet ID configured for {evaluator_name}")

    # 1. Get spreadsheet info to find the first tab's ID
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    ws_id = spreadsheet['sheets'][0]['properties']['sheetId']
    ws_title = spreadsheet['sheets'][0]['properties']['title']

    # 2. Clear old data
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{ws_title}!A1:F1000"
    ).execute()

    # 3. Rename tab to Evlauate (if not already) and set frozen row
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"updateSheetProperties": {
            "properties": {"sheetId": ws_id, "title": "Evaluate", "gridProperties": {"frozenRowCount": 1}},
            "fields": "title,gridProperties.frozenRowCount"
        }}]}
    ).execute()

    # 4. Build header + data rows
    header = ["#", "Recording Link", "Transcript", "Call Summary", "Your Category", "Resolved?"]
    rows = [header]
    for i, call in enumerate(calls, 1):
        rec_url = call["recording_url"] if call["recording_url"] != "N/A" else ""
        text = call["transcript"][:40000]
        summary = call.get("ai_summary", "") or ""
        rows.append([i, rec_url, text, summary, "", ""])

    # 5. Write new data
    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="Evaluate!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    # 6. Add data validation and formatting
    category_values = [{"userEnteredValue": c} for c in CATEGORIES]
    resolved_values = [{"userEnteredValue": v} for v in ["Yes", "No", "Partial"]]

    requests_list = [
        # Category dropdown (column E, index 4)
        {
            "setDataValidation": {
                "range": {"sheetId": ws_id, "startRowIndex": 1, "endRowIndex": NUM_CALLS + 1, "startColumnIndex": 4, "endColumnIndex": 5},
                "rule": {"condition": {"type": "ONE_OF_LIST", "values": category_values}, "showCustomUi": True, "strict": True}
            }
        },
        # Resolved dropdown (column F, index 5)
        {
            "setDataValidation": {
                "range": {"sheetId": ws_id, "startRowIndex": 1, "endRowIndex": NUM_CALLS + 1, "startColumnIndex": 5, "endColumnIndex": 6},
                "rule": {"condition": {"type": "ONE_OF_LIST", "values": resolved_values}, "showCustomUi": True, "strict": True}
            }
        },
        # Column resizing
        {"autoResizeDimensions": {"dimensions": {"sheetId": ws_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 2}}},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 3},
            "properties": {"pixelSize": 600}, "fields": "pixelSize"
        }},
        # Summary column width
        {"updateDimensionProperties": {
            "range": {"sheetId": ws_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": 4},
            "properties": {"pixelSize": 400}, "fields": "pixelSize"
        }},
        # Category + Resolved column width
        {"updateDimensionProperties": {
            "range": {"sheetId": ws_id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 6},
            "properties": {"pixelSize": 200}, "fields": "pixelSize"
        }},
        # Formatting
        {"repeatCell": {
            "range": {"sheetId": ws_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }},
        # Wrap text in transcript + summary columns
        {"repeatCell": {
            "range": {"sheetId": ws_id, "startRowIndex": 1, "endRowIndex": NUM_CALLS + 1, "startColumnIndex": 2, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat.wrapStrategy"
        }},
    ]

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": requests_list}
    ).execute()
    
    return sheet_id


def populate_score_sheet(sheets_service, sheet_id, round_number, calls):
    """Write AI answers to the score sheet for later comparison."""
    if not sheet_id:
        raise ValueError("No SCORE_SHEET_ID configured")

    # 1. Get spreadsheet info
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    existing_sheets = {s['properties']['title']: s['properties']['sheetId'] for s in spreadsheet['sheets']}

    # 2. Ensure "AI Answers" and "Results" tabs exist
    requests_list = []
    if "AI Answers" not in existing_sheets:
        requests_list.append({"addSheet": {"properties": {"title": "AI Answers"}}})
    if "Results" not in existing_sheets:
        requests_list.append({"addSheet": {"properties": {"title": "Results"}}})
        
    if requests_list:
        sheets_service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests_list}).execute()

    # 3. Clear existing AI Answers
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range="AI Answers!A1:E1000"
    ).execute()

    # 4. Write new AI answers
    header = ["#", "Call ID", "AI Category", "AI Resolved", "AI Summary"]
    rows = [header]
    for i, call in enumerate(calls, 1):
        rows.append([
            i,
            call["call_id"],
            call["ai_category"],
            call["ai_resolved"],
            call["ai_summary"],
        ])

    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="AI Answers!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    return sheet_id


# ============================================
# SLACK
# ============================================

def send_slack_message(text, blocks=None):
    """Send a message to the Slack channel."""
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=text,
            blocks=blocks,
        )
        return True
    except SlackApiError as e:
        print(f"  Slack error: {e.response['error']}")
        return False


def post_eval_ready(round_number, sheet_links):
    """Post eval notification to Slack with individual sheet links."""
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📋 Weekly Eval Round #{round_number} is Ready!"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "Please categorize 10 calls this week. Pick the *issue_category* and *resolved* status for each call.\n\nYour individual sheets (only you can see yours):"}
        },
    ]

    for name, sheet_id in sheet_links.items():
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"• *{name}*: <{url}|Open your sheet>"}
        })

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "⏰ *Deadline: Friday 6 PM*\nListen to the recordings and/or read the transcripts, then select categories from the dropdown."}
    })

    fallback_text = f"Eval Round #{round_number} is ready! Check your sheets."
    return send_slack_message(fallback_text, blocks=blocks)


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Generate weekly eval sheets")
    parser.add_argument("--dry-run", action="store_true", help="Preview calls, no sheets/slack")
    parser.add_argument("--test-slack", action="store_true", help="Send a test Slack message")
    args = parser.parse_args()

    if args.test_slack:
        if not SLACK_BOT_TOKEN:
            print("SLACK_BOT_TOKEN is required for --test-slack.", file=sys.stderr)
            sys.exit(1)
        print("Sending test message to Slack...")
        ok = send_slack_message("🧪 Test message from Call Eval Bot — everything works!")
        print("✅ Sent!" if ok else "❌ Failed!")
        return

    print("=" * 60)
    print("  EVAL GENERATE: Weekly Call Evaluation")
    print("=" * 60)

    if not DATABASE_URL:
        print("DATABASE_URL is required.", file=sys.stderr)
        sys.exit(1)

    # DB setup
    conn = get_db_connection()
    print("  ✅ Connected to DB")
    ensure_eval_table(conn)

    round_number = get_next_round_number(conn)
    print(f"  Round: #{round_number}")

    # Pick calls
    calls = pick_eval_calls(conn, count=NUM_CALLS)
    print(f"  Selected: {len(calls)} calls")

    if len(calls) < NUM_CALLS:
        print(f"  ⚠️  Only found {len(calls)} eligible calls (need {NUM_CALLS})")
        if len(calls) == 0:
            print("  ❌ No calls available. Exiting.")
            conn.close()
            return

    if args.dry_run:
        print("\n  --- DRY RUN: Selected calls ---")
        for i, c in enumerate(calls, 1):
            print(f"  [{i}] {c['call_id']} → AI: {c['ai_category']} ({c['ai_resolved']})")
            print(f"      Transcript: {c['transcript'][:100]}...")
        print(f"\n  Would update {len(EVAL_SHEETS)} sheets + 1 score sheet")
        for name, sid in EVAL_SHEETS.items():
            print(f"    {name}: {sid}")
        conn.close()
        return

    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        print("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required (unless --dry-run).", file=sys.stderr)
        conn.close()
        sys.exit(1)

    # Create Google Sheets
    print("\n  Populating Google Sheets...")
    sheets_svc = get_sheets_service()

    sheet_links = {}
    for name, sid in EVAL_SHEETS.items():
        print(f"    📄 {name}...", end=" ", flush=True)
        try:
            populate_eval_sheet(sheets_svc, sid, name, round_number, calls)
            sheet_links[name] = sid
            print(f"✅ Indexed")
        except Exception as e:
            print(f"❌ Error: {e}")

    print(f"    📊 Score sheet...", end=" ", flush=True)
    try:
        score_sid = populate_score_sheet(sheets_svc, SCORE_SHEET_ID, round_number, calls)
        print(f"✅ Indexed")
    except Exception as e:
        print(f"❌ Error: {e}")
        score_sid = SCORE_SHEET_ID

    # Save to DB
    call_ids = [c["call_id"] for c in calls]
    save_eval_round(conn, round_number, call_ids, sheet_links, score_sid)
    print(f"  ✅ Eval round #{round_number} saved to DB")

    # Post to Slack
    print("\n  Posting to Slack...")
    ok = post_eval_ready(round_number, sheet_links)
    print("  ✅ Slack notification sent!" if ok else "  ❌ Slack notification failed!")

    conn.close()

    print(f"\n{'='*60}")
    print(f"  ✅ EVAL ROUND #{round_number} GENERATED")
    print(f"  Sheets: {len(sheet_links)} individual + 1 score")
    print(f"  Calls: {len(calls)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
