"""
Fetch Ozonetel CloudAgent CDRs and insert into the same "smartflo data" Postgres table
as scripts/fetch_calls.py (Tata Tele / Smartflo). Ozonetel field values are normalized to the
same *enums and shapes* Smartflo uses (e.g. Call Status Answered/Missed, Direction Inbound/Outbound)
so dashboards and SQL that assume Tata data keep working.

Uses Ozonetel reporting APIs (NOT DeleteBulkData — that only removes campaign upload data):
  - Generate Token: POST .../ca_apis/CAToken/generateToken
  - CDR pagination: GET .../ca_reports/fetchCdrByPagination (JSON body + query params)

Docs:
  https://docs.ozonetel.com/reference/post_ca-apis-catoken-generatetoken
  https://docs.ozonetel.com/reference/get_ca-reports-fetchcdrbypagination

Environment:
  OZONETEL_API_KEY   — CloudAgent API key (header apiKey)
  OZONETEL_USERNAME  — CloudAgent username (same as Tata flow’s account identity)
  OZONETEL_DOMAIN    — optional, default in1-ccaas-api.ozonetel.com (domestic CCaaS)

Rate limit: Ozonetel allows ~2 CDR requests/minute — this script sleeps between pages.
`fromDate` / `toDate` must be the same calendar day (docs); you can narrow to an hour
for lighter hourly jobs (`--ist-current-hour` or `--time-from` / `--time-to`).

Incremental fetch (default): reads MAX(Time) for Ozonetel rows already in the requested window
and moves `fromDate` forward (with a short overlap) so hourly cron does not re-paginate the
full hour. Use `--no-incremental` for a full backfill. Overlap minutes: env
`OZONETEL_INCREMENTAL_OVERLAP_MINUTES` (default 3).

Inbound vs outbound: see `infer_ozonetel_direction()` — uses Event, CallFlow, Type.

Rapid-retry dedup (on by default): Ozonetel logs one CDR per redial even when a
caller repeatedly hangs up inside the IVR. If the same customer number has
``>= OZONETEL_DEDUP_THRESHOLD`` (default 5) CDRs inside a single fetch window,
we keep only the earliest Answered + earliest Missed row and drop the rest
before insert. Turn off with ``--no-dedup`` or tune with ``--dedup-threshold N``
(env ``OZONETEL_DEDUP_THRESHOLD``). See ``dedup_rapid_retries()``.

Usage:
  python scripts/fetch_ozonetel_calls.py --date 2026-03-29
  python scripts/fetch_ozonetel_calls.py --days 1 --dry-run
  python scripts/fetch_ozonetel_calls.py --ist-current-hour
  python scripts/fetch_ozonetel_calls.py --date 2026-03-30 --time-from 17:00:00 --time-to 17:59:59
  python scripts/fetch_ozonetel_calls.py --yesterday   # full prior IST day → DB (needs migration 001)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

import requests
from psycopg2.extras import Json

from fetch_calls import (
    CDR_SOURCE_COLUMN,
    CDR_SOURCE_OZONETEL,
    DATABASE_URL,
    format_duration,
    get_db_connection,
    get_existing_call_ids,
    get_max_ozonetel_time_in_window,
    insert_call_records,
)

IST = timezone(timedelta(hours=5, minutes=30))

OZONETEL_DOMAIN = (
    os.environ.get("OZONETEL_DOMAIN", "").strip() or "in1-ccaas-api.ozonetel.com"
)
OZONETEL_API_KEY = os.environ.get("OZONETEL_API_KEY", "").strip()
OZONETEL_USERNAME = os.environ.get("OZONETEL_USERNAME", "").strip()

# CDR API is capped at ~2 req/min per Ozonetel docs
CDR_REQUEST_INTERVAL_SEC = 31

# Re-fetch this much before MAX(Time) so late-arriving CDR / clock skew still land in the slice
_DEFAULT_OVERLAP_MIN = 3
OZONETEL_INCREMENTAL_OVERLAP_MIN = int(
    os.environ.get("OZONETEL_INCREMENTAL_OVERLAP_MINUTES", str(_DEFAULT_OVERLAP_MIN)) or _DEFAULT_OVERLAP_MIN
)


def _parse_window_dt(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")


def _format_window_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _naive_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if getattr(value, "tzinfo", None):
        return value.replace(tzinfo=None)
    return value


def narrow_fetch_window_for_incremental(
    window_from: str,
    window_to: str,
    db_max_time: datetime | None,
    overlap_minutes: int,
) -> tuple[str, str] | None:
    """
    If we already have rows up to db_max_time in this window, shrink fromDate toward the tail
    (with overlap) like Tata's \"stop when older than target\" — fewer pages on hourly runs.

    Returns (from_str, to_str) or None if there is nothing left to request.
    """
    if db_max_time is None:
        return window_from, window_to
    wf = _parse_window_dt(window_from)
    wt = _parse_window_dt(window_to)
    mx = _naive_dt(db_max_time)
    if mx is None:
        return window_from, window_to
    overlap = timedelta(minutes=max(0, overlap_minutes))
    new_from = max(wf, mx - overlap)
    if new_from > wt:
        return None
    if new_from == wf:
        return window_from, window_to
    return _format_window_dt(new_from), window_to


def generate_token(domain: str, api_key: str, username: str) -> str:
    url = f"https://{domain}/ca_apis/CAToken/generateToken"
    r = requests.post(
        url,
        headers={"Content-Type": "application/json", "apiKey": api_key},
        json={"userName": username},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    tok = data.get("token")
    if not tok or data.get("status") == "false":
        raise RuntimeError(data.get("message") or "Token generation failed")
    return tok


def _extract_recording_url(raw: str | None) -> str:
    if not raw or not str(raw).strip():
        return "N/A"
    s = str(raw).strip()
    m = re.search(r"https?://[^\s\"'<>]+", s)
    return m.group(0).rstrip(".,)\"") if m else (s if s.startswith("http") else "N/A")


def _parse_hms_to_seconds(value: str | None) -> int | None:
    if not value or not isinstance(value, str):
        return None
    parts = value.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        return h * 3600 + m * 60 + s
    except ValueError:
        return None


def _parse_customer_number(caller_id: str | None, e164: str | None) -> int | None:
    for raw in (caller_id, e164):
        if not raw:
            continue
        digits = re.sub(r"\D", "", str(raw))
        if digits:
            try:
                return int(digits)
            except ValueError:
                pass
    return None


def _parse_did(did: str | None) -> int | None:
    if not did:
        return None
    digits = re.sub(r"\D", "", str(did))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _combine_datetime(call_date: str | None, hhmmss: str | None) -> datetime | None:
    if not call_date or not hhmmss:
        return None
    try:
        return datetime.strptime(f"{call_date.strip()} {hhmmss.strip()}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def infer_ozonetel_direction(detail: dict) -> str:
    """
    Ozonetel direction heuristics:

    - **Inbound:** Type=InBound AND CampaignName=Customer_support.
      We require the campaign name check because other queues (Testride, DID-named)
      are not true inbound CS calls and should stay Outbound.

    - **Outbound:** Event=AgentDial, or Type is a dialer mode
      (Progressive, Predictive, Preview, Manual, etc.).

    Returns: \"Inbound\", \"Outbound\", or \"Unknown\".
    """
    ev = (detail.get("Event") or "").strip().lower()
    cf = (detail.get("CallFlow") or "").strip().lower()
    typ = (detail.get("Type") or "").strip().lower()
    campaign = (detail.get("CampaignName") or "").strip().lower()

    # Inbound only if Type signals inbound AND campaign is Customer_support
    if ("inbound" in typ or "inbound" in ev or "inbound" in cf or "incoming" in ev or "incoming" in cf) \
            and campaign == "customer_support":
        return "Inbound"

    outbound_types = ("progressive", "predictive", "preview", "manual", "power", "autodial")
    if ev == "agentdial" or "agentdial" in cf:
        return "Outbound"
    if typ in outbound_types:
        return "Outbound"

    return "Unknown"


# Smartflo API uses lowercase status then fetch_calls applies .capitalize() -> "Answered", "Missed", ...
# Ozonetel uses different strings (e.g. Unanswered); map them to the same stored values as Tata.
_OZONETEL_STATUS_TO_SMARTFLO: dict[str, str] = {
    "answered": "Answered",
    "unanswered": "Missed",
    "not_answered": "Missed",
    "not answered": "Missed",
    "no_answer": "Missed",
    "no answer": "Missed",
    "busy": "Missed",
    "failed": "Missed",
    "cancelled": "Missed",
    "canceled": "Missed",
    "rejected": "Missed",
    "congestion": "Missed",
    "ringing": "Missed",
    "missed": "Missed",
}


def normalize_ozonetel_status_to_smartflo(raw: str) -> str:
    """Align Ozonetel Status with Smartflo `status` after capitalize (Answered, Missed, …)."""
    if not raw or not str(raw).strip():
        return "N/A"
    key = re.sub(r"\s+", "_", str(raw).strip().lower())
    if key in _OZONETEL_STATUS_TO_SMARTFLO:
        return _OZONETEL_STATUS_TO_SMARTFLO[key]
    # Unknown status: same rule as Tata — single-word capitalize
    return str(raw).strip().capitalize()


def normalize_ozonetel_direction_to_smartflo(direction: str, detail: dict) -> str:
    """
    Smartflo stores Direction as `Inbound` or `Outbound` (from lowercase API via .capitalize()).
    Ozonetel `Unknown` is resolved to Outbound when the row is clearly dialer-driven.
    """
    if direction == "Inbound":
        return "Inbound"
    if direction == "Outbound":
        return "Outbound"
    typ = (detail.get("Type") or "").strip().lower()
    ev = (detail.get("Event") or "").strip().lower()
    if typ in ("progressive", "predictive", "preview", "manual", "power", "autodial"):
        return "Outbound"
    if ev == "agentdial" or "agentdial" in (detail.get("CallFlow") or "").lower():
        return "Outbound"
    return "Outbound"


def normalize_ozonetel_hangup_cause(raw: str | None) -> str:
    """Roughly match Smartflo hangup_cause style (human-readable phrase, not CamelCase)."""
    if not raw or not str(raw).strip():
        return "N/A"
    s = str(raw).strip()
    if " " in s:
        return s
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", s).strip()
    return spaced or s


def _reason_key_from_ozonetel(detail: dict) -> str:
    """Smartflo `reason` is often snake_case; derive from HangupBy / dial outcome when possible."""
    hb = (detail.get("HangupBy") or "").strip()
    if hb:
        s = re.sub(r"([a-z])([A-Z])", r"\1_\2", hb)
        return s.lower().replace(" ", "_") or "N/A"
    ds = (detail.get("DialStatus") or "").strip().lower().replace(" ", "_")
    if ds and ds not in ("answered", ""):
        return ds
    return "N/A"


def _call_solution_like_tata(direction: str) -> str:
    """Match fetch_calls.map_api_to_db Call Solution rules (Dialer / Incoming / N/A)."""
    d = (direction or "").lower()
    if d == "inbound":
        return "Incoming"
    if d == "outbound":
        return "Dialer"
    return "N/A"


def _uui_to_customer_name_and_ref(uui: str | None) -> tuple[str, str]:
    """
    Tata splits contact name vs ref_id. UUI is sometimes a person name, sometimes an id.
    - Looks like a person name (letters + space) -> Customer Name, External ref N/A
    - Otherwise non-empty -> External Reference ID, Customer Name N/A
    """
    if not uui or not str(uui).strip():
        return "N/A", "N/A"
    u = str(uui).strip()
    if " " in u and re.search(r"[A-Za-z]", u):
        return u, "N/A"
    return "N/A", u


def _recording_basename(url: str) -> str:
    if not url or url == "N/A":
        return "N/A"
    path = url.split("?", 1)[0].rstrip("/")
    base = path.split("/")[-1] if path else ""
    return base if base else "N/A"


def format_ozonetel_call_flow_like_tata(detail: dict) -> str:
    """
    Tata uses comma-separated text from format_call_flow(); approximate the same style
    so dashboards see a similar string (not JSON).
    """
    parts: list[str] = []
    st = (detail.get("StartTime") or "") or "N/A"
    cust_st = (detail.get("CustomerDialStatus") or "").lower()
    if cust_st == "answered":
        parts.append(f"Customer: (Answer)({st})")
    elif detail.get("CustomerDialStatus"):
        label = str(detail.get("CustomerDialStatus")).replace("+", " ")
        parts.append(f"Customer: ({label})({st})")
    ev = str(detail.get("Event") or "").replace("+", " ")
    cf = str(detail.get("CallFlow") or "").replace("+", " ")
    ag = detail.get("AgentName") or ""
    pk = detail.get("PickupTime") or st
    camp = detail.get("CampaignName") or ag or ""
    leg = f"{ev or cf}: {camp}({pk})".strip()
    if leg and leg != ": ()":
        parts.append(leg)
    if detail.get("HangupBy"):
        et = detail.get("EndTime") or ""
        parts.append(f"Hangup: ({et})")
    return ", ".join(parts) if parts else "N/A"


def _agent_hangup_like_tata(detail: dict) -> str:
    """Tata uses agent_hangup_data name: status; best-effort from Ozonetel."""
    hb_raw = (detail.get("HangupBy") or "").strip()
    hb = normalize_ozonetel_hangup_cause(hb_raw) if hb_raw else "N/A"
    ag = (detail.get("AgentName") or "").strip()
    if hb_raw.lower().replace(" ", "") == "agenthangup" and ag:
        return f"{ag}: {hb}"
    if hb != "N/A" and ag:
        return f"{ag}: {hb}"
    return "N/A"


def map_ozonetel_detail_to_db(detail: dict) -> dict:
    """
    Map one Ozonetel CDR `details[]` object to the same columns and *conventions* as
    scripts/fetch_calls.map_api_to_db (Tata Tele): same \"N/A\" defaults, same duration
    semantics, text Call Flow, Disposition in Disposition Names (Dialer Disposition stays
    N/A like Tata), Call Start/Answered/End Time left N/A like Tata's API mapping.
    """
    ucid = str(detail.get("UCID") or detail.get("CallID") or "").strip()
    call_date = detail.get("CallDate") or ""
    start_t = detail.get("StartTime") or detail.get("PickupTime") or "00:00:00"
    timestamp = _combine_datetime(call_date, start_t)

    talk_sec = _parse_hms_to_seconds(detail.get("TalkTime"))
    duration_sec = _parse_hms_to_seconds(detail.get("Duration"))
    if talk_sec is None and duration_sec is not None:
        talk_sec = duration_sec

    direction = normalize_ozonetel_direction_to_smartflo(
        infer_ozonetel_direction(detail), detail
    )
    direction_lower = direction.lower()

    status_raw = (detail.get("Status") or "").strip()
    call_status = normalize_ozonetel_status_to_smartflo(status_raw)

    recording = _extract_recording_url(detail.get("CallAudio"))
    cust_name, ext_ref = _uui_to_customer_name_and_ref(detail.get("UUI"))
    disp = (detail.get("Disposition") or "").strip()
    disp_names = disp if disp else "N/A"
    comments = (detail.get("Comments") or "").strip()

    return {
        "Direction": direction,
        "Call Status": call_status,
        "Call ID": Json(ucid),
        "Time": timestamp,
        "Customer Number": _parse_customer_number(detail.get("CallerID"), detail.get("E164")),
        "Customer Name": cust_name,
        "Call Solution": _call_solution_like_tata(direction),
        "DID": _parse_did(detail.get("DID")),
        "Agent Number": detail.get("AgentID") or "N/A",
        "Answered By Agent": detail.get("AgentName") or "N/A",
        "Hangup Cause": normalize_ozonetel_hangup_cause(detail.get("HangupBy")),
        "Agent Hangup Cause": _agent_hangup_like_tata(detail),
        "Campaign ID": "N/A",
        "Campaign Name": detail.get("CampaignName") or "N/A",
        "Call Start Time": "N/A",
        "Call Answered Time": "N/A",
        "Call End Time": "N/A",
        "Conversation Duration": format_duration(talk_sec),
        "Call Duration": format_duration(duration_sec if duration_sec is not None else talk_sec),
        "Call Flow": format_ozonetel_call_flow_like_tata(detail),
        "Missed By Agent(s)": "N/A",
        "Recording File Name": _recording_basename(recording),
        "Recording": recording,
        "SIP Response Code": "N/A",
        "Reason Key": _reason_key_from_ozonetel(detail),
        "Circle": (detail.get("Location") or "").strip() or "N/A",
        "Operator": "N/A",
        "Call Scope": "domestic",
        "Auto Attendant": "N/A",
        "Ivr": "N/A",
        "Department": "N/A",
        "Voicemail": "N/A",
        "Time Group": "N/A",
        "Notes": comments if comments else "N/A",
        "Agent Ring Time": detail.get("CustomerRingTime") or detail.get("TimeToAnswer") or "N/A",
        "Lead ID": "N/A",
        "List ID": "N/A",
        "List Name": "N/A",
        "Inbound Queue": "N/A",
        "Wrap up Duration": "N/A",
        "Hold Duration": detail.get("HoldDuration") or "0:00:00",
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
        "Disposition Codes": "N/A",
        "External Reference ID": ext_ref,
        "AMD Status": "N/A",
        "Inbound Duration": format_duration(duration_sec)
        if direction_lower == "inbound"
        else "0:00:00",
        "Outbound Duration": format_duration(talk_sec)
        if direction_lower == "outbound"
        else "0:00:00",
        "Call Sub Type": "N/A",
        "Wait Duration": "0:00:00",
        "Answer Duration": "0:00:00",
        "Skill ID": "N/A",
        "Skill Name": detail.get("Skill") or "N/A",
        "DTMF": "N/A",
        "Dial Assist Score (%)": "N/A",
        CDR_SOURCE_COLUMN: CDR_SOURCE_OZONETEL,
    }


def fetch_cdr_page(
    domain: str,
    token: str,
    api_key: str,
    username: str,
    from_datetime: str,
    to_datetime: str,
    page_no: int,
    page_size: int,
) -> tuple[list[dict], int]:
    """
    Returns (details_rows, total_count_hint).
    Uses GET with a JSON body (POST returns 405 on domestic CCaaS).
    """
    body = {
        "fromDate": from_datetime,
        "toDate": to_datetime,
        "userName": username,
    }
    url = f"https://{domain}/ca_reports/fetchCdrByPagination"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "apiKey": api_key,
    }
    params = {"pageNo": page_no, "pageSize": page_size}

    def _parse(resp: requests.Response) -> tuple[list[dict], int]:
        resp.raise_for_status()
        data = resp.json()
        st = str(data.get("status", "")).lower()
        if st in ("fail", "false", "error"):
            msg = data.get("message") or str(data)
            raise RuntimeError(f"CDR API error: {msg}")
        details = data.get("details") or data.get("data") or []
        total = int(data.get("totalCount") or len(details))
        if not isinstance(details, list):
            details = []
        return details, total

    r = requests.request(
        "GET",
        url,
        params=params,
        headers=headers,
        data=json.dumps(body),
        timeout=120,
    )
    return _parse(r)


# Rapid-retry dedup: if the same customer number appears many times in one fetch
# window, it's almost always the IVR-drop redial pattern Ozonetel doesn't dedup
# on their side. We keep the earliest Answered + earliest Missed and drop the rest.
OZONETEL_DEDUP_THRESHOLD = int(os.environ.get("OZONETEL_DEDUP_THRESHOLD", "5"))


def _detail_sort_key(detail: dict) -> datetime:
    ts = _combine_datetime(detail.get("CallDate") or "", detail.get("StartTime") or "00:00:00")
    return ts or datetime.max


def dedup_rapid_retries(
    details: list[dict],
    threshold: int = OZONETEL_DEDUP_THRESHOLD,
) -> tuple[list[dict], dict[int, int]]:
    """
    Collapse rapid-retry CDRs inside a single fetch window.

    For any customer number with ``>= threshold`` rows in ``details``, keep only:
      - the earliest ``Answered`` row (if any), and
      - the earliest non-answered row (Unanswered/Missed/Busy/…).
    All other rows for that customer are dropped from the list before insert.

    Returns ``(filtered_details, dropped_counts_by_number)``.
    """
    from collections import defaultdict

    groups: dict[int, list[dict]] = defaultdict(list)
    for d in details:
        num = _parse_customer_number(d.get("CallerID"), d.get("E164"))
        if num is None:
            continue
        groups[num].append(d)

    drop_ucids: set[str] = set()
    dropped_by_customer: dict[int, int] = {}

    for num, rows in groups.items():
        if len(rows) < threshold:
            continue

        rows_sorted = sorted(rows, key=_detail_sort_key)
        earliest_answered: dict | None = None
        earliest_missed: dict | None = None
        for r in rows_sorted:
            raw_status = (r.get("Status") or "").strip().lower()
            if raw_status == "answered":
                if earliest_answered is None:
                    earliest_answered = r
            else:
                if earliest_missed is None:
                    earliest_missed = r
            if earliest_answered is not None and earliest_missed is not None:
                break

        keep_ucids: set[str] = set()
        for keeper in (earliest_answered, earliest_missed):
            if keeper is None:
                continue
            ucid = str(keeper.get("UCID") or keeper.get("CallID") or "").strip()
            if ucid:
                keep_ucids.add(ucid)

        drops_for_num = 0
        for r in rows:
            ucid = str(r.get("UCID") or r.get("CallID") or "").strip()
            if not ucid or ucid in keep_ucids:
                continue
            drop_ucids.add(ucid)
            drops_for_num += 1
        if drops_for_num:
            dropped_by_customer[num] = drops_for_num

    if not drop_ucids:
        return details, {}

    filtered = [
        d
        for d in details
        if str(d.get("UCID") or d.get("CallID") or "").strip() not in drop_ucids
    ]
    return filtered, dropped_by_customer


def fetch_all_for_window(
    domain: str,
    token: str,
    api_key: str,
    username: str,
    from_datetime: str,
    to_datetime: str,
    page_size: int = 100,
    max_records: int | None = None,
) -> list[dict]:
    all_rows: list[dict] = []
    page = 1
    total_hint: int | None = None

    while True:
        print(f"    CDR page {page} ({from_datetime} → {to_datetime})...", flush=True)
        details, total = fetch_cdr_page(
            domain,
            token,
            api_key,
            username,
            from_datetime,
            to_datetime,
            page,
            page_size,
        )
        if total_hint is None:
            total_hint = total
        all_rows.extend(details)
        if max_records and len(all_rows) >= max_records:
            return all_rows[:max_records]

        if not details or len(details) < page_size:
            break
        if total_hint and len(all_rows) >= total_hint:
            break

        page += 1
        time.sleep(CDR_REQUEST_INTERVAL_SEC)

    return all_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Ozonetel CDR → Postgres (smartflo data table)")
    parser.add_argument("--date", help="Single day YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=1, help="With no --date: today + this many days back")
    parser.add_argument("--limit", type=int, default=None, help="Max CDR rows per day (testing)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument(
        "--time-from",
        metavar="HH:MM:SS",
        help="With --date only: start time on that day (same-day window with --time-to)",
    )
    parser.add_argument(
        "--time-to",
        metavar="HH:MM:SS",
        help="With --date only: end time on that day",
    )
    parser.add_argument(
        "--ist-current-hour",
        action="store_true",
        help="Fetch only the current clock hour in IST for today (good for hourly cron)",
    )
    parser.add_argument(
        "--ist-previous-hour",
        action="store_true",
        help="Fetch the previous full hour in IST for today (cron at :05 to catch stragglers)",
    )
    parser.add_argument(
        "--yesterday",
        action="store_true",
        help="Fetch the full previous calendar day in IST (00:00–23:59) and insert into Postgres",
    )
    parser.add_argument(
        "--outbound-only",
        action="store_true",
        help="After mapping, insert only rows with Direction = Outbound (skip Inbound/Unknown)",
    )
    parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Always request the full from/to window (no tail slice from MAX(Time)); use for backfill/repair",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Disable rapid-retry dedup (keep every CDR Ozonetel returns).",
    )
    parser.add_argument(
        "--dedup-threshold",
        type=int,
        default=OZONETEL_DEDUP_THRESHOLD,
        help=f"Dedup a customer's calls in a window when count >= this (default {OZONETEL_DEDUP_THRESHOLD}).",
    )
    args = parser.parse_args()

    if not OZONETEL_API_KEY or not OZONETEL_USERNAME:
        print(
            "Set OZONETEL_API_KEY and OZONETEL_USERNAME (and optionally OZONETEL_DOMAIN).",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.ist_current_hour and args.ist_previous_hour:
        print("Use only one of --ist-current-hour / --ist-previous-hour.", file=sys.stderr)
        sys.exit(1)
    if args.yesterday and (
        args.date
        or args.time_from
        or args.time_to
        or args.ist_current_hour
        or args.ist_previous_hour
    ):
        print(
            "Do not combine --yesterday with --date, --time-from/--time-to, or --ist-*-hour.",
            file=sys.stderr,
        )
        sys.exit(1)
    if (args.time_from or args.time_to) and not args.date:
        print("--time-from / --time-to require --date.", file=sys.stderr)
        sys.exit(1)
    if (args.time_from and not args.time_to) or (args.time_to and not args.time_from):
        print("Provide both --time-from and --time-to with --date.", file=sys.stderr)
        sys.exit(1)

    # List of (label, from_datetime, to_datetime) — all same calendar day per Ozonetel rules
    windows: list[tuple[str, str, str]] = []

    if args.yesterday:
        y = (datetime.now(IST) - timedelta(days=1)).strftime("%Y-%m-%d")
        windows.append((f"{y} (IST yesterday)", f"{y} 00:00:00", f"{y} 23:59:59"))
    elif args.ist_current_hour or args.ist_previous_hour:
        now = datetime.now(IST)
        if args.ist_previous_hour:
            now = now - timedelta(hours=1)
        day_str = now.strftime("%Y-%m-%d")
        h = now.hour
        windows.append(
            (
                f"{day_str} H{h:02d} IST",
                f"{day_str} {h:02d}:00:00",
                f"{day_str} {h:02d}:59:59",
            )
        )
    elif args.date and args.time_from and args.time_to:
        windows.append(
            (
                f"{args.date} {args.time_from}-{args.time_to}",
                f"{args.date} {args.time_from}",
                f"{args.date} {args.time_to}",
            )
        )
    elif args.date:
        windows.append(
            (
                args.date,
                f"{args.date} 00:00:00",
                f"{args.date} 23:59:59",
            )
        )
    else:
        for i in range(0, args.days + 1):
            d = (datetime.now(IST) - timedelta(days=i)).strftime("%Y-%m-%d")
            windows.append((d, f"{d} 00:00:00", f"{d} 23:59:59"))

    print("=" * 60)
    print("  OZONETEL CDR → POSTGRES (smartflo data)")
    print(f"  Domain: {OZONETEL_DOMAIN}")
    print(f"  Windows: {len(windows)}")
    for label, fr, to in windows:
        print(f"    • {label}: {fr} → {to}")
    print("=" * 60)

    if not args.dry_run and not DATABASE_URL:
        print("DATABASE_URL missing.", file=sys.stderr)
        sys.exit(1)
    if not args.dry_run:
        print("  Postgres: short-lived connections (existing IDs + insert) — avoids pooler idle timeout during long CDR fetch.")
        print(
            f'  Apply scripts/migrations/001_add_cdr_source.sql if needed (column "{CDR_SOURCE_COLUMN}").'
        )

    token = generate_token(OZONETEL_DOMAIN, OZONETEL_API_KEY, OZONETEL_USERNAME)
    print("  Token OK (valid ~60m per Ozonetel docs)")

    grand_inserted = 0
    grand_details = 0

    for label, from_dt, to_dt in windows:
        day_str = from_dt[:10]
        print(f"\n  --- {label} ---")
        existing = set()
        fetch_from, fetch_to = from_dt, to_dt
        if not args.dry_run:
            id_conn = get_db_connection()
            try:
                existing = get_existing_call_ids(id_conn, day_str)
                if not args.no_incremental:
                    db_max = get_max_ozonetel_time_in_window(id_conn, from_dt, to_dt)
                    narrowed = narrow_fetch_window_for_incremental(
                        from_dt,
                        to_dt,
                        db_max,
                        OZONETEL_INCREMENTAL_OVERLAP_MIN,
                    )
                    if narrowed is None:
                        print(
                            f"    Incremental: already at end of window (max in DB covers {from_dt} → {to_dt}). Skipping fetch."
                        )
                        time.sleep(CDR_REQUEST_INTERVAL_SEC)
                        continue
                    fetch_from, fetch_to = narrowed
                    if (fetch_from, fetch_to) != (from_dt, to_dt):
                        print(
                            f"    Incremental slice: {fetch_from} → {fetch_to} "
                            f"(full window was {from_dt} → {to_dt}; overlap {OZONETEL_INCREMENTAL_OVERLAP_MIN}m)"
                        )
            finally:
                id_conn.close()
            print(f"    Existing Call IDs (around {day_str}): {len(existing)}")

        time.sleep(CDR_REQUEST_INTERVAL_SEC)
        try:
            details = fetch_all_for_window(
                OZONETEL_DOMAIN,
                token,
                OZONETEL_API_KEY,
                OZONETEL_USERNAME,
                fetch_from,
                fetch_to,
                page_size=args.page_size,
                max_records=args.limit,
            )
        except Exception as e:
            print(f"    ❌ Fetch failed: {e}")
            continue

        grand_details += len(details)
        print(f"    Fetched CDR rows: {len(details)}")

        if not args.no_dedup and details:
            before = len(details)
            details, dropped = dedup_rapid_retries(details, threshold=args.dedup_threshold)
            if dropped:
                total_dropped = sum(dropped.values())
                print(
                    f"    Rapid-retry dedup: dropped {total_dropped} row(s) "
                    f"across {len(dropped)} customer(s) "
                    f"(threshold >= {args.dedup_threshold}); kept {len(details)}/{before}"
                )
                sample = sorted(dropped.items(), key=lambda kv: -kv[1])[:5]
                for num, n in sample:
                    print(f"      {num}: -{n}")

        db_rows = []
        skipped_ob = 0
        for d in details:
            cid = str(d.get("UCID") or d.get("CallID") or "").strip()
            if not cid or cid in existing:
                continue
            row = map_ozonetel_detail_to_db(d)
            if args.outbound_only and row.get("Direction") != "Outbound":
                skipped_ob += 1
                continue
            db_rows.append(row)
            existing.add(cid)

        if args.outbound_only and skipped_ob:
            print(f"    Skipped (not Outbound): {skipped_ob}")
        print(f"    New rows to insert: {len(db_rows)}")
        if args.dry_run and db_rows:
            sample = db_rows[0]
            for k in (
                "Direction",
                "Call Status",
                "Time",
                "Recording",
                "Answered By Agent",
                CDR_SOURCE_COLUMN,
            ):
                print(f"      {k}: {sample.get(k)}")
        elif not args.dry_run and db_rows:
            # Retry up to 3 times on SSL/connection drops (Supabase pooler can close
            # idle connections during the CDR fetch window).
            for attempt in range(3):
                ins_conn = get_db_connection()
                try:
                    n = insert_call_records(ins_conn, db_rows)
                    grand_inserted += n
                    print(f"    Inserted: {n} (attempted {len(db_rows)})")
                    break
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    print(f"    ⚠️  DB connection error (attempt {attempt + 1}/3): {e}")
                    try:
                        ins_conn.close()
                    except Exception:
                        pass
                    if attempt < 2:
                        time.sleep(5)
                    else:
                        print(f"    ❌ Insert failed after 3 attempts, skipping batch.")
                else:
                    try:
                        ins_conn.close()
                    except Exception:
                        pass

        time.sleep(CDR_REQUEST_INTERVAL_SEC)

    print(f"\n{'='*60}")
    print(f"  Done. CDR rows fetched: {grand_details}  |  Inserted this run: {grand_inserted}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
