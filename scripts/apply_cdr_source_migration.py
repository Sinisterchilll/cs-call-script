#!/usr/bin/env python3
"""
Apply scripts/migrations/001_add_cdr_source.sql DDL when the Supabase SQL editor times out.

The "smartflo data" table often has long-running readers/writers; ALTER needs a brief
ACCESS EXCLUSIVE lock. This script retries with lock_timeout on the session pooler
(port 5432), not the transaction pooler (6543).

Usage:
  export DATABASE_URL="postgresql://...@....pooler.supabase.com:5432/postgres"
  python scripts/apply_cdr_source_migration.py

If DATABASE_URL uses :6543/, this script rewrites to :5432/ for DDL (session mode).
"""

from __future__ import annotations

import os
import re
import sys
import time

try:
    import psycopg2
except ImportError:
    print("Install: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)


def session_mode_url(url: str) -> str:
    """Supabase transaction pooler (6543) is a poor fit for DDL; prefer 5432."""
    return re.sub(r":6543/", ":5432/", url)


def main() -> None:
    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        print(
            "Set DATABASE_URL (session pooler :5432 recommended for DDL).",
            file=sys.stderr,
        )
        sys.exit(1)
    url = session_mode_url(raw)
    if url != raw:
        print("Note: using port 5432 (session pooler) for DDL instead of 6543.")

    attempts = int(os.environ.get("CDR_MIGRATION_ATTEMPTS", "120"))
    pause_sec = float(os.environ.get("CDR_MIGRATION_PAUSE_SEC", "2"))

    for i in range(1, attempts + 1):
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("SET statement_timeout = 0")
            cur.execute("SET lock_timeout = '5s'")
            cur.execute(
                'ALTER TABLE "smartflo data" ADD COLUMN IF NOT EXISTS "CDR Source" text'
            )
            cur.execute(
                """COMMENT ON COLUMN "smartflo data"."CDR Source" IS """
                """'cdr provider: tata = Tata Smartflo API, ozonetel = Ozonetel CDR API'"""
            )
            print(f"OK: CDR Source column added (attempt {i}).")
            cur.close()
            conn.close()
            return
        except Exception as e:
            cur.close()
            conn.close()
            err = str(e).lower()
            if "lock" in err or "timeout" in err:
                print(f"  attempt {i}/{attempts}: waiting for table lock… ({e!r})")
                time.sleep(pause_sec)
                continue
            print(f"FAILED: {e}", file=sys.stderr)
            sys.exit(1)

    print(
        "Could not acquire lock after all attempts. "
        "Pause GitHub fetch/analyze jobs and retry, or run during low traffic.",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
