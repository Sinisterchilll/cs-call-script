-- Ozonetel logical-call metrics (matches CloudAgent / India dashboard when UCIDs are present).
-- Replace :day (YYYY-MM-DD) and campaign as needed.
--
-- Rules:
--   • India calendar day: ("Time" AT TIME ZONE 'Asia/Kolkata')::date
--   • Exclude empty "Ozonetel UCID" (junk / non-reconcilable legs)
--   • Logical call = TRIM(BOTH '"' FROM "Call ID"::text)
--   • Answered = any leg Answered for that Call ID

WITH legs AS (
  SELECT
    TRIM(BOTH '"' FROM "Call ID"::text) AS cid,
    "Call Status"
  FROM "smartflo data"
  WHERE "CDR Source" = 'ozonetel'
    AND ("Time" AT TIME ZONE 'Asia/Kolkata')::date = '2026-04-24'  -- :day
    AND "Campaign Name" = 'Customer_support'                        -- :campaign
    AND "Ozonetel UCID" IS NOT NULL
    AND TRIM("Ozonetel UCID") != ''
    AND TRIM(BOTH '"' FROM "Call ID"::text) != ''
),
logical AS (
  SELECT
    cid,
    BOOL_OR("Call Status" = 'Answered') AS any_answered
  FROM legs
  GROUP BY cid
)
SELECT
  COUNT(*) AS total_logical,
  COUNT(*) FILTER (WHERE any_answered) AS answered,
  COUNT(*) FILTER (WHERE NOT any_answered) AS missed
FROM logical;

-- Outside working hours (IST): first touch MIN("Time") per cid not in [08:00, 22:00)
-- WITH legs AS (
--   SELECT TRIM(BOTH '"' FROM "Call ID"::text) AS cid, "Call Status", "Time"
--   FROM "smartflo data"
--   WHERE "CDR Source" = 'ozonetel'
--     AND ("Time" AT TIME ZONE 'Asia/Kolkata')::date = :day
--     AND "Campaign Name" = :campaign
--     AND "Ozonetel UCID" IS NOT NULL AND TRIM("Ozonetel UCID") != ''
--     AND TRIM(BOTH '"' FROM "Call ID"::text) != ''
-- ),
-- per_call AS (
--   SELECT cid, BOOL_OR("Call Status" = 'Answered') AS any_answered,
--          (MIN("Time") AT TIME ZONE 'Asia/Kolkata')::time AS call_ist_time
--   FROM legs GROUP BY cid
-- )
-- SELECT COUNT(*) FILTER (WHERE NOT (call_ist_time >= time '08:00' AND call_ist_time < time '22:00')),
--        ...
