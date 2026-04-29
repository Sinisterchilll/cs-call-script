-- Unique Ozonetel leg id (UCID) for deduplication when "Call ID" holds parent CallID.
-- Run once before using updated fetch_ozonetel_calls.py / backfill scripts.

ALTER TABLE "smartflo data"
  ADD COLUMN IF NOT EXISTS "Ozonetel UCID" text;

COMMENT ON COLUMN "smartflo data"."Ozonetel UCID" IS
  'Ozonetel fetchCDRDetails UCID (one per leg). Used to dedupe inserts; Call ID stores parent CallID for reporting.';
