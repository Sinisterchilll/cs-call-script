"""
Analyze call recordings: Transcribe with ElevenLabs → Analyze with GPT-4o Batch API.
Transcripts cached in DB to avoid re-transcription on failures.

Usage:
    python scripts/analyze_calls.py                       # Analyze all pending
    python scripts/analyze_calls.py --limit 5             # Test with 5 calls
    python scripts/analyze_calls.py --transcribe-only     # Only transcribe, skip GPT
    python scripts/analyze_calls.py --analyze-only        # Only GPT batch (transcripts must exist)
    python scripts/analyze_calls.py --date 2026-03-01     # Only calls from this date
"""

import os
import sys
import json
import argparse
import tempfile
import time
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta, timezone
from io import BytesIO

# ============================================
# CONFIGURATION
# ============================================

ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres.lrnxhtplcuogmbtbzfmx:RaSIDQjfdzBzSiYJ@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"
)

TABLE_CALLS = "smartflo data"
TABLE_TRANSCRIPTS = "call_transcripts"
TABLE_ANALYSIS = "call analysis"

RATE_LIMIT_DELAY = 0.5  # Between ElevenLabs calls

# ============================================
# 30 SINGLE-LEVEL CATEGORIES
# ============================================

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

CATEGORY_DESCRIPTIONS = """
- refund_issue: Customer asking about refund status, requesting refund, refund not received
- recharge_issue: Problems with recharge, balance not updated, recharge failed
- payment_failure: Payment didn't go through, transaction failed, double charged
- billing_dispute: Wrong charges, unexpected deductions, billing clarification
- penalty_or_fine: Fines for late return, damage charges, penalty disputes
- kyc_document_issue: Document rejected, wrong document uploaded, re-upload needed
- kyc_pending: KYC verification pending/delayed, status inquiry
- kyc_guidance: How to do KYC, what documents needed, KYC process help
- otp_or_login_issue: OTP not received, can't login, account locked, password reset
- plan_pricing_inquiry: Asking about rental plans, pricing, subscription costs
- onboarding_or_deposit_fee: Questions about deposit amount, onboarding charges, security deposit
- offer_or_promotion: Asking about current offers, discounts, promo codes
- outbound_offer_call: Agent calling to offer plans/promotions (outbound sales)
- bike_wont_start: Vehicle won't start, ignition problem, can't unlock bike
- bike_stopping_mid_ride: Vehicle stopped during ride, sudden shutdown
- battery_swap_issue: Battery swap station problems, swap failed, no battery available
- battery_range_issue: Low range, battery draining fast, battery percentage mismatch
- vehicle_repair_or_damage: Vehicle damaged, needs repair, maintenance request
- vehicle_missing: Vehicle not found at location, stolen, misplaced
- hub_location_inquiry: Asking where nearest hub/station is, hub address
- vehicle_availability: Checking if vehicles available at a location
- vehicle_pickup_or_return: Issues picking up or returning vehicle, return location
- hub_service_delay: Long wait at hub, slow service, hub staff issues
- app_error_or_block: App crashing, not loading, blocked account, technical errors
- app_data_mismatch: Wrong info in app, ride history mismatch, incorrect display
- referral_issue: Referral bonus not received, referral code not working
- no_response_or_disconnected: Call dropped, no one spoke, blank call
- general_inquiry: General questions, greetings, doesn't fit other categories
- language_barrier: Communication difficult due to language mismatch
- service_complaint: General dissatisfaction, escalation request, poor service feedback
- police: Police related issues, traffic fines, vehicle impounded
- escalation: Escalation requests, will be contacted back
"""

GPT_SYSTEM_PROMPT = "You are a call analysis engine for an EV rental company. Return only valid JSON."

GPT_USER_PROMPT_TEMPLATE = """Analyze this call center transcript. Return ONLY valid JSON.

Transcript:
{transcript}

Return this exact JSON structure:
{{
  "issue_category": "<one of the categories listed below>",
  "resolved": "<Yes/No/Partial>",
  "language": "<detected language>",
  "summary": "<one clear sentence summarizing what the call was about and what happened>"
}}

Categories (pick exactly ONE):
{categories}

Category definitions:
{category_descriptions}

Rules:
- issue_category: Pick exactly ONE most relevant category.
- resolved: "Yes" if resolved, "No" if not, "Partial" if partially addressed.
- language: Primary language (hindi/english/tamil/telugu/kannada/marathi/bengali/gujarati/malayalam/punjabi/odia/urdu). If mixed, pick dominant.
- summary: One concise sentence. Be specific about issue and outcome.

Only return valid JSON, nothing else."""


# ============================================
# DATABASE
# ============================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_calls_needing_transcription(conn, date_str=None, limit=None):
    """Get answered calls with recordings that haven't been transcribed yet."""
    sql = f"""
        SELECT TRIM(BOTH '"' FROM sd."Call ID"::text) as call_id, sd."Recording"
        FROM "{TABLE_CALLS}" sd
        LEFT JOIN {TABLE_TRANSCRIPTS} ct
            ON ct.call_id = TRIM(BOTH '"' FROM sd."Call ID"::text)
        WHERE ct.call_id IS NULL
          AND sd."Call Status" = 'Answered'
          AND sd."Recording" IS NOT NULL
          AND sd."Recording" != 'N/A'
          AND sd."Time" >= '2026-03-01'
    """
    params = []
    if date_str:
        sql += ' AND sd."Time"::date = %s'
        params.append(date_str)
    sql += ' ORDER BY sd."Time" DESC'
    if limit:
        sql += ' LIMIT %s'
        params.append(limit)

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def get_transcripts_needing_analysis(conn, date_str=None, limit=None):
    """Get transcripts that haven't been analyzed yet."""
    sql = f"""
        SELECT ct.call_id, ct.transcript
        FROM {TABLE_TRANSCRIPTS} ct
        LEFT JOIN "{TABLE_ANALYSIS}" ca ON ca.activity_id = ct.call_id
        WHERE ca.activity_id IS NULL
          AND ct.transcript IS NOT NULL
          AND LENGTH(ct.transcript) > 20
    """
    params = []
    if date_str:
        sql += ' AND ct.transcribed_at::date = %s'
        params.append(date_str)
    if limit:
        sql += ' LIMIT %s'
        params.append(limit)

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def save_transcript(conn, call_id, transcript, language):
    """Save transcript to DB."""
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {TABLE_TRANSCRIPTS} (call_id, transcript, language) VALUES (%s, %s, %s) "
        f"ON CONFLICT (call_id) DO NOTHING",
        (call_id, transcript, language)
    )
    conn.commit()


def save_analysis(conn, call_id, issue_category, resolved, language, summary):
    """Save analysis result to DB."""
    cur = conn.cursor()
    
    # Check if exists first (since there's no UNIQUE constraint on activity_id)
    cur.execute(f'SELECT 1 FROM "{TABLE_ANALYSIS}" WHERE activity_id = %s', (call_id,))
    if cur.fetchone():
        return

    cur.execute(
        f'INSERT INTO "{TABLE_ANALYSIS}" (activity_id, issue_category, resolved, language, summary) '
        f"VALUES (%s, %s, %s, %s, %s)",
        (call_id, issue_category, resolved, language, summary)
    )
    conn.commit()


# ============================================
# STAGE 1: DOWNLOAD + TRANSCRIBE
# ============================================

def download_recording(recording_url):
    """Download recording to a temp file. Returns file path or None."""
    try:
        resp = requests.get(recording_url, timeout=60, stream=True)
        resp.raise_for_status()

        # Check it's actually audio
        content_type = resp.headers.get('content-type', '')
        if 'text/html' in content_type:
            return None

        tmp = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
        for chunk in resp.iter_content(chunk_size=8192):
            tmp.write(chunk)
        tmp.close()

        # Skip tiny files (< 5KB = likely error page)
        if os.path.getsize(tmp.name) < 5000:
            os.unlink(tmp.name)
            return None

        return tmp.name
    except Exception as e:
        print(f"    Download error: {str(e)[:80]}")
        return None


def transcribe_recording(audio_path):
    """Transcribe audio with ElevenLabs Scribe v2. Returns (transcript, language) or None."""
    try:
        from elevenlabs import ElevenLabs
        client = ElevenLabs(api_key=ELEVENLABS_API_KEY)

        with open(audio_path, 'rb') as f:
            audio_data = BytesIO(f.read())

        result = client.speech_to_text.convert(
            file=audio_data,
            model_id="scribe_v2",
            tag_audio_events=True,
            language_code=None,
            diarize=True,
        )

        language = getattr(result, 'language_code', 'unknown')

        # Build diarized transcript
        if hasattr(result, 'words') and result.words:
            segments = []
            current_speaker = None
            current_words = []
            for word in result.words:
                speaker = getattr(word, 'speaker_id', None)
                if speaker != current_speaker and current_words:
                    label = f"Speaker {current_speaker}" if current_speaker is not None else "Speaker"
                    segments.append(f"{label}: {' '.join(current_words)}")
                    current_words = []
                current_speaker = speaker
                current_words.append(word.text)
            if current_words:
                label = f"Speaker {current_speaker}" if current_speaker is not None else "Speaker"
                segments.append(f"{label}: {' '.join(current_words)}")
            transcript = "\n".join(segments)
        else:
            transcript = result.text if hasattr(result, 'text') else str(result)

        return transcript, language

    except Exception as e:
        err_msg = str(e)
        if 'quota_exceeded' in err_msg.lower():
            return "QUOTA_EXHAUSTED", None
        print(f"    Transcription error: {err_msg[:100]}")
        return None, None


def stage_transcribe(conn, date_str=None, limit=None):
    """Stage 1: Download recordings and transcribe with ElevenLabs."""
    print(f"\n{'='*60}")
    print(f"  STAGE 1: DOWNLOAD + TRANSCRIBE (ElevenLabs Scribe v2)")
    print(f"{'='*60}")

    calls = get_calls_needing_transcription(conn, date_str=date_str, limit=limit)
    print(f"  Calls needing transcription: {len(calls)}")

    if not calls:
        print(f"  Nothing to transcribe.")
        return

    success = 0
    failed = 0
    for i, (call_id, recording_url) in enumerate(calls):
        print(f"  [{i+1}/{len(calls)}] {call_id}...", end=" ", flush=True)

        # Download
        audio_path = download_recording(recording_url)
        if not audio_path:
            print("❌ download failed")
            failed += 1
            continue

        # Transcribe
        transcript, language = transcribe_recording(audio_path)

        # Cleanup temp file immediately
        try:
            os.unlink(audio_path)
        except:
            pass

        if transcript == "QUOTA_EXHAUSTED":
            print("⚠️  ElevenLabs quota exhausted! Stopping.")
            break

        if not transcript:
            print("❌ transcription failed")
            failed += 1
            continue

        # Save to DB
        save_transcript(conn, call_id, transcript, language)
        success += 1
        print(f"✅ ({language}, {len(transcript)} chars)")

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  Transcribed: {success}, Failed: {failed}")


# ============================================
# STAGE 2: GPT BATCH ANALYSIS
# ============================================

def build_batch_jsonl(transcripts):
    """Build JSONL content for GPT Batch API."""
    lines = []
    categories_str = ", ".join(CATEGORIES)

    for call_id, transcript in transcripts:
        user_prompt = GPT_USER_PROMPT_TEMPLATE.format(
            transcript=transcript[:8000],  # Truncate very long transcripts
            categories=categories_str,
            category_descriptions=CATEGORY_DESCRIPTIONS,
        )

        request_obj = {
            "custom_id": call_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": GPT_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            },
        }
        lines.append(json.dumps(request_obj, ensure_ascii=False))

    return "\n".join(lines)


def upload_batch_file(jsonl_content):
    """Upload JSONL file to OpenAI."""
    import openai
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Write to temp file
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8')
    tmp.write(jsonl_content)
    tmp.close()

    try:
        with open(tmp.name, 'rb') as f:
            file_obj = client.files.create(file=f, purpose="batch")
        return file_obj.id
    finally:
        os.unlink(tmp.name)


def create_and_wait_for_batch(file_id):
    """Create batch and poll until complete."""
    import openai
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    batch = client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    print(f"    Batch created: {batch.id}")
    print(f"    Status: {batch.status}")

    # Poll for completion
    while batch.status in ("validating", "in_progress", "finalizing"):
        time.sleep(30)
        batch = client.batches.retrieve(batch.id)
        completed = batch.request_counts.completed if batch.request_counts else 0
        total = batch.request_counts.total if batch.request_counts else 0
        print(f"    Status: {batch.status} ({completed}/{total} done)")

    if batch.status != "completed":
        print(f"    ❌ Batch failed: {batch.status}")
        if batch.errors:
            for err in batch.errors.data[:5]:
                print(f"      Error: {err.message}")
        return None

    return batch


def download_batch_results(batch):
    """Download and parse batch results."""
    import openai
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    output_file_id = batch.output_file_id
    if not output_file_id:
        print("    ❌ No output file")
        return {}

    content = client.files.content(output_file_id)
    results = {}

    for line in content.text.strip().split("\n"):
        if not line.strip():
            continue
        obj = json.loads(line)
        custom_id = obj.get("custom_id", "")
        response_body = obj.get("response", {}).get("body", {})
        choices = response_body.get("choices", [])

        if choices:
            message_content = choices[0].get("message", {}).get("content", "")
            try:
                # Strip markdown fences if present
                text = message_content.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3]
                    text = text.strip()

                parsed = json.loads(text)
                results[custom_id] = parsed
            except json.JSONDecodeError:
                print(f"    ⚠️  JSON parse error for {custom_id}")
                results[custom_id] = None
        else:
            error = obj.get("error", {})
            print(f"    ⚠️  Error for {custom_id}: {error}")
            results[custom_id] = None

    return results


def validate_analysis(result):
    """Validate and normalize GPT analysis result."""
    if not result or not isinstance(result, dict):
        return None

    category = result.get("issue_category", "general_inquiry").lower().strip()
    if category not in CATEGORIES:
        category = "general_inquiry"

    resolved = result.get("resolved", "No").strip().capitalize()
    if resolved not in ("Yes", "No", "Partial"):
        resolved = "No"

    language = result.get("language", "unknown").lower().strip()
    summary = result.get("summary", "").strip()
    if not summary:
        summary = "No summary available."

    return {
        "issue_category": category,
        "resolved": resolved,
        "language": language,
        "summary": summary,
    }


def stage_analyze(conn, date_str=None, limit=None):
    """Stage 2: Batch analyze transcripts with GPT-4o."""
    print(f"\n{'='*60}")
    print(f"  STAGE 2: GPT-4o BATCH ANALYSIS")
    print(f"{'='*60}")

    transcripts = get_transcripts_needing_analysis(conn, date_str=date_str, limit=limit)
    print(f"  Transcripts needing analysis: {len(transcripts)}")

    if not transcripts:
        print(f"  Nothing to analyze.")
        return

    # Build JSONL
    print(f"  Building batch request ({len(transcripts)} calls)...")
    jsonl = build_batch_jsonl(transcripts)

    # Upload
    print(f"  Uploading to OpenAI...")
    file_id = upload_batch_file(jsonl)
    print(f"    File ID: {file_id}")

    # Create batch and wait
    print(f"  Submitting batch...")
    batch = create_and_wait_for_batch(file_id)
    if not batch:
        return

    # Download results
    print(f"  Downloading results...")
    results = download_batch_results(batch)
    print(f"    Got {len(results)} results")

    # Insert into DB
    success = 0
    failed = 0
    for call_id, result in results.items():
        validated = validate_analysis(result)
        if validated:
            save_analysis(
                conn, call_id,
                validated["issue_category"],
                validated["resolved"],
                validated["language"],
                validated["summary"],
            )
            success += 1
        else:
            failed += 1

    print(f"  ✅ Analyzed: {success}, Failed: {failed}")
    completed = batch.request_counts.completed if batch.request_counts else 0
    total_tokens = getattr(batch, 'usage', None)
    if total_tokens:
        print(f"  Tokens used: {total_tokens}")


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Analyze call recordings")
    parser.add_argument("--date", help="Only process calls from this date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Max calls to process")
    parser.add_argument("--transcribe-only", action="store_true", help="Only transcribe, skip GPT")
    parser.add_argument("--analyze-only", action="store_true", help="Only GPT batch (transcripts must exist)")
    args = parser.parse_args()

    print("=" * 60)
    print("  CALL ANALYSIS PIPELINE")
    print("  ElevenLabs Scribe v2 + GPT-4o Batch API")
    print("=" * 60)

    conn = get_db_connection()
    print(f"  ✅ Connected to Postgres")

    if args.date:
        print(f"  Date filter: {args.date}")
    if args.limit:
        print(f"  Limit: {args.limit}")

    if not args.analyze_only:
        if not ELEVENLABS_API_KEY:
            print("  ❌ ELEVENLABS_API_KEY not set!")
            sys.exit(1)
        stage_transcribe(conn, date_str=args.date, limit=args.limit)

    if not args.transcribe_only:
        if not OPENAI_API_KEY:
            print("  ❌ OPENAI_API_KEY not set!")
            sys.exit(1)
        stage_analyze(conn, date_str=args.date, limit=args.limit)

    # Summary
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_TRANSCRIPTS}")
    t_count = cur.fetchone()[0]
    cur.execute(f'SELECT COUNT(*) FROM "{TABLE_ANALYSIS}"')
    a_count = cur.fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"  Total transcripts in DB: {t_count}")
    print(f"  Total analyses in DB:    {a_count}")

    conn.close()


if __name__ == "__main__":
    main()
