-- CDR provider tag for "smartflo data" rows (Tata Smartflo vs Ozonetel CloudAgent).
-- Run once in Supabase SQL editor or psql before using updated fetch scripts.

ALTER TABLE "smartflo data"
  ADD COLUMN IF NOT EXISTS "CDR Source" text;

COMMENT ON COLUMN "smartflo data"."CDR Source" IS
  'cdr provider: tata = Tata Smartflo API, ozonetel = Ozonetel CDR API';

-- Optional: mark existing rows as Tata (comment out if you already have mixed sources).
UPDATE "smartflo data"
SET "CDR Source" = 'tata'
WHERE "CDR Source" IS NULL;

-- Rollback / cleanup test load (example — only Ozonetel rows):
-- DELETE FROM "smartflo data" WHERE "CDR Source" = 'ozonetel';
