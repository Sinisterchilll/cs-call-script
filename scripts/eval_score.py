"""
Weekly Eval: Score responses from team members.

Reads individual sheets, compares against AI categories, and:
- Writes results to the Score Sheet
- Posts summary report to Slack

Usage:
    python scripts/eval_score.py                  # Score the latest round
    python scripts/eval_score.py --round 3        # Score a specific round
    python scripts/eval_score.py --dry-run        # Preview scores, no writes
"""

import os
import sys
import json
import argparse
import psycopg2
from datetime import datetime
from collections import Counter
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

EVAL_SHEETS = {
    "Prudhvi": os.environ.get("SHEET_ID_PRUDHVI", "1ViguRl61iO6eiN1tyHRkHxXitqf8Iglf452s4FFceB4"),
    "Aum": os.environ.get("SHEET_ID_AUM", "1hn5-C5wW8b-O_z7zNFeD-vTDhvyfRMU0rxxwQ7m_LaY"),
    "Ayush": os.environ.get("SHEET_ID_AYUSH", "1UNjafjwQyfNoOATsfjG4q-N2udOXaa4dhRepaPJat8w"),
    "Swarna": os.environ.get("SHEET_ID_SWARNA", "1Mi4wEc9Pxd7LkxOlcUeHJhTMrtsHHgkizoNTwrmewyE"),
    "Uditi": os.environ.get("SHEET_ID_UDITI", "168-BRsA0s1LWtpKz_7loocDvNpFLpoHJdJb4ALpeeBI"),
}

SCORE_SHEET_ID = os.environ.get("SHEET_ID_SCORES", "1DR3Ud-Pqu12T_QVU5VECa75w6zw9OVWWQMtPQPazIoE")
EVALUATORS = list(EVAL_SHEETS.keys())


# ============================================
# DATABASE
# ============================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_latest_round(conn, round_number=None):
    """Get the latest (or specific) eval round."""
    cur = conn.cursor()
    if round_number:
        cur.execute("SELECT id, round_number, call_ids, sheet_ids, score_sheet_id FROM eval_rounds WHERE round_number = %s", (round_number,))
    else:
        cur.execute("SELECT id, round_number, call_ids, sheet_ids, score_sheet_id FROM eval_rounds ORDER BY round_number DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "round_number": row[1],
        "call_ids": row[2],
        "sheet_ids": row[3],
        "score_sheet_id": row[4],
    }


def get_ai_results(conn, call_ids):
    """Get AI analysis results for the given call IDs."""
    cur = conn.cursor()
    placeholders = ", ".join(["%s"] * len(call_ids))
    cur.execute(f"""
        SELECT activity_id, issue_category, resolved
        FROM "call analysis"
        WHERE activity_id IN ({placeholders})
    """, call_ids)
    return {row[0]: {"category": row[1], "resolved": row[2]} for row in cur.fetchall()}


def mark_round_scored(conn, round_id):
    """Mark this round as scored."""
    cur = conn.cursor()
    cur.execute("UPDATE eval_rounds SET scored_at = NOW() WHERE id = %s", (round_id,))
    conn.commit()


# ============================================
# GOOGLE SHEETS
# ============================================

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


def read_evaluator_responses(sheets_service, sheet_id):
    """Read category + resolved responses from an evaluator's sheet."""
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="Evaluate!A2:F11",  # Rows 2-11 (10 calls), cols A-F
    ).execute()

    rows = result.get("values", [])
    responses = []
    for row in rows:
        # Columns: #, Recording Link, Transcript, Call Summary, Category, Resolved
        category = row[4].strip().lower() if len(row) > 4 and row[4] else ""
        resolved = row[5].strip().capitalize() if len(row) > 5 and row[5] else ""
        responses.append({"category": category, "resolved": resolved})

    return responses


def write_results_to_score_sheet(sheets_service, score_sheet_id, round_number, call_ids, ai_results, all_responses):
    """Write detailed results to the Results tab of the score sheet."""
    # Build header
    header = ["#", "Call ID", "AI Category"]
    for name in EVALUATORS:
        header.append(f"{name} Category")
    header.extend(["Majority Vote", "AI Correct?", "Agreement Count"])

    rows = [header]
    ai_correct_count = 0
    total_scored = 0

    for i, call_id in enumerate(call_ids):
        ai = ai_results.get(call_id, {})
        ai_cat = ai.get("category", "?")

        # Gather human picks
        human_picks = []
        row = [i + 1, call_id, ai_cat]
        for name in EVALUATORS:
            pick = ""
            if name in all_responses and i < len(all_responses[name]):
                pick = all_responses[name][i].get("category", "")
            row.append(pick)
            if pick:
                human_picks.append(pick)

        # Majority vote
        if human_picks:
            majority = Counter(human_picks).most_common(1)[0][0]
            agreement_count = Counter(human_picks).most_common(1)[0][1]
            ai_match = "✅" if ai_cat == majority else "❌"
            if ai_cat == majority:
                ai_correct_count += 1
            total_scored += 1
        else:
            majority = "N/A"
            agreement_count = 0
            ai_match = "—"

        row.extend([majority, ai_match, agreement_count])
        rows.append(row)

    # Summary row
    accuracy = f"{ai_correct_count / total_scored * 100:.0f}%" if total_scored > 0 else "N/A"
    rows.append([])
    rows.append(["", "", f"AI Accuracy: {accuracy} ({ai_correct_count}/{total_scored})"])

    # Per-person agreement with AI
    rows.append([])
    rows.append(["", "", "Per-person agreement with AI:"])
    for name in EVALUATORS:
        agree = 0
        total = 0
        for i, call_id in enumerate(call_ids):
            ai = ai_results.get(call_id, {})
            if name in all_responses and i < len(all_responses[name]):
                pick = all_responses[name][i].get("category", "")
                if pick:
                    total += 1
                    if pick == ai.get("category", ""):
                        agree += 1
        pct = f"{agree/total*100:.0f}%" if total > 0 else "N/A"
        rows.append(["", "", f"  {name}: {pct} ({agree}/{total})"])

    # Write
    tab_name = f"Round {round_number}"
    # Add a new tab
    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=score_sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        ).execute()
    except Exception:
        # Tab might already exist
        tab_name = "Results"

    sheets_service.spreadsheets().values().update(
        spreadsheetId=score_sheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    return ai_correct_count, total_scored


# ============================================
# SLACK
# ============================================

def post_eval_results(round_number, ai_correct, total, per_person, confusion_pairs):
    """Post scoring results to Slack."""
    accuracy = f"{ai_correct / total * 100:.0f}%" if total > 0 else "N/A"

    person_lines = []
    for name, stats in per_person.items():
        pct = f"{stats['agree']}/{stats['total']}" if stats["total"] > 0 else "N/A"
        person_lines.append(f"  {name}: {pct}")

    confusion_text = ""
    if confusion_pairs:
        top_3 = confusion_pairs[:3]
        confusion_text = "\n".join(f"  {ai_cat} ↔ {human_cat} ({count}x)" for (ai_cat, human_cat), count in top_3)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊 Eval Round #{round_number} Results"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*AI Accuracy (vs majority vote):* {accuracy} ({ai_correct}/{total})"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Human vs AI Agreement:*\n```\n" + "\n".join(person_lines) + "\n```"}
        },
    ]

    if confusion_text:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Most confused categories:*\n```\n" + confusion_text + "\n```"}
        })

    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=f"Eval Round #{round_number} Results: AI accuracy {accuracy}",
            blocks=blocks,
        )
        return True
    except SlackApiError as e:
        print(f"  Slack error: {e.response['error']}")
        return False


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Score eval round")
    parser.add_argument("--round", type=int, help="Specific round number to score")
    parser.add_argument("--dry-run", action="store_true", help="Preview scores only")
    args = parser.parse_args()

    print("=" * 60)
    print("  EVAL SCORE: Weekly Call Evaluation Results")
    print("=" * 60)

    if not DATABASE_URL:
        print("DATABASE_URL is required.", file=sys.stderr)
        sys.exit(1)

    conn = get_db_connection()
    print("  ✅ Connected to DB")

    # Get round info
    eval_round = get_latest_round(conn, round_number=args.round)
    if not eval_round:
        print("  ❌ No eval round found")
        conn.close()
        return

    round_number = eval_round["round_number"]
    call_ids = eval_round["call_ids"]
    sheet_ids = eval_round["sheet_ids"]  # {"Prudhvi": "sheet_id", ...}
    score_sheet_id = eval_round["score_sheet_id"]

    print(f"  Round: #{round_number}")
    print(f"  Calls: {len(call_ids)}")
    print(f"  Evaluators: {len(sheet_ids)}")

    # Get AI results
    ai_results = get_ai_results(conn, call_ids)
    print(f"  AI results loaded: {len(ai_results)}")

    # Read evaluator responses
    print("\n  Reading evaluator sheets...")
    sheets_svc = get_sheets_service()
    all_responses = {}

    for name, sid in sheet_ids.items():
        print(f"    📄 {name}...", end=" ", flush=True)
        try:
            responses = read_evaluator_responses(sheets_svc, sid)
            filled = sum(1 for r in responses if r["category"])
            all_responses[name] = responses
            print(f"✅ ({filled}/{len(call_ids)} filled)")
        except Exception as e:
            print(f"❌ Error: {e}")
            all_responses[name] = []

    # Compute scores
    print("\n  Computing scores...")
    per_person = {}
    confusion_counter = Counter()

    for name in EVALUATORS:
        agree = 0
        total = 0
        for i, call_id in enumerate(call_ids):
            ai = ai_results.get(call_id, {})
            if name in all_responses and i < len(all_responses[name]):
                pick = all_responses[name][i].get("category", "")
                if pick:
                    total += 1
                    if pick == ai.get("category", ""):
                        agree += 1
                    else:
                        confusion_counter[(ai.get("category", ""), pick)] += 1
        per_person[name] = {"agree": agree, "total": total}

    # Majority vote accuracy
    ai_correct = 0
    total_scored = 0
    for i, call_id in enumerate(call_ids):
        ai = ai_results.get(call_id, {})
        picks = []
        for name in EVALUATORS:
            if name in all_responses and i < len(all_responses[name]):
                pick = all_responses[name][i].get("category", "")
                if pick:
                    picks.append(pick)
        if picks:
            majority = Counter(picks).most_common(1)[0][0]
            total_scored += 1
            if ai.get("category") == majority:
                ai_correct += 1

    accuracy = f"{ai_correct / total_scored * 100:.0f}%" if total_scored > 0 else "N/A"

    # Print results
    print(f"\n  {'='*50}")
    print(f"  AI Accuracy (vs majority): {accuracy} ({ai_correct}/{total_scored})")
    print(f"  {'─'*50}")
    for name, stats in per_person.items():
        pct = f"{stats['agree']}/{stats['total']}" if stats["total"] > 0 else "no responses"
        print(f"    {name}: {pct}")
    print(f"  {'─'*50}")
    if confusion_counter:
        print(f"  Top confused categories:")
        for (ai_cat, human_cat), count in confusion_counter.most_common(5):
            print(f"    AI: {ai_cat} → Human: {human_cat} ({count}x)")
    print(f"  {'='*50}")

    if args.dry_run:
        print("\n  🔍 DRY RUN — no writes made")
        conn.close()
        return

    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        print("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required (unless --dry-run).", file=sys.stderr)
        conn.close()
        sys.exit(1)

    # Write to score sheet
    print("\n  Writing to score sheet...")
    write_results_to_score_sheet(sheets_svc, score_sheet_id, round_number, call_ids, ai_results, all_responses)
    print("  ✅ Score sheet updated")

    # Mark round as scored
    mark_round_scored(conn, eval_round["id"])

    # Post to Slack
    print("  Posting results to Slack...")
    confusion_pairs = confusion_counter.most_common(5)
    ok = post_eval_results(round_number, ai_correct, total_scored, per_person, confusion_pairs)
    print("  ✅ Slack report sent!" if ok else "  ❌ Slack report failed!")

    conn.close()

    print(f"\n{'='*60}")
    print(f"  ✅ EVAL ROUND #{round_number} SCORED")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
