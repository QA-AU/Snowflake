-- =============================================================================
-- SCD2 POST-LOAD VALIDATION — SNOWFLAKE
-- =============================================================================
--
-- PURPOSE:
--   Validates that SCD2 was correctly applied to TGT_TABLE for a given
--   BUSINESS_DATE. No writes to TGT_TABLE at any point.
--   All SCD2 simulation is done in temp tables and compared against
--   what actually exists in TGT_TABLE after the load.
--
-- WHAT THIS SCRIPT DOES (run top to bottom in sequence):
--   STEP 1 : Pull today's active source records into TMP_SRC
--   STEP 2 : Pull D-1 active target records into TMP_TGT_PREV
--            (reads rows active on BUSINESS_DATE - 1, i.e. pre-load baseline)
--   STEP 3 : Classify every record as INSERT / UPDATE / DELETE into TMP_CLASSIFIED
--   STEP 4 : Simulate SCD2 output into TMP_EXPECTED
--            (what the target SHOULD look like for BUSINESS_DATE after a correct load)
--   STEP 5 : Compare TMP_EXPECTED vs TGT_TABLE — MINUS in both directions
--            to catch missing rows, extra rows, and wrong values
--   STEP 6 : Summary — one result set showing PASS/FAIL per check type
--
-- READS FROM  : SRC_TABLE, TGT_TABLE (read-only, never modified)
-- WRITES TO   : temp tables only (TMP_SRC, TMP_TGT_PREV, TMP_CLASSIFIED, TMP_EXPECTED)
--
-- THINGS YOU MUST UPDATE (marked with !! UPDATE !!):
--   1. BUSINESS_DATE       -- line ~55  : the date you are validating
--   2. SRC_TABLE           -- STEP 1    : your source table name
--   3. TGT_TABLE           -- STEP 2, 5 : your target table name (read-only)
--   4. KEY_COL1, KEY_COL2  -- throughout : your composite key columns
--   5. COL_A, COL_B, COL_C -- throughout : your payload columns
--
-- INTERPRETING RESULTS (STEP 6):
--   MISSING_FROM_TARGET : rows expected in target but not found  → load missed them
--   EXTRA_IN_TARGET     : rows in target not expected by logic   → load wrote extras
--   Each check returns DISCREPANCY_COUNT and STATUS (PASS / FAIL).
--   All checks must be PASS for the load to be considered correct.
-- =============================================================================


-- =============================================================================
-- !! UPDATE 1 OF 5 !!
-- SET BUSINESS DATE — the date whose SCD2 load you want to validate
-- =============================================================================
SET BUSINESS_DATE = '2026-02-21';    -- <-- !! UPDATE THIS DATE BEFORE RUNNING !!
-- =============================================================================


-- =============================================================================
-- STEP 1: PULL TODAY'S ACTIVE SOURCE RECORDS INTO TMP_SRC
-- -----------------------------------------------------------------------------
-- Reads source records active on BUSINESS_DATE, excludes deleted ones.
-- Computes RECORD_HASH on all non-key, non-audit payload columns.
--
-- !! UPDATE 2 OF 5 !! -- Replace SRC_TABLE with your actual source table name
-- !! UPDATE 4 OF 5 !! -- Replace KEY_COL1, KEY_COL2 with your composite key cols
-- !! UPDATE 5 OF 5 !! -- Replace COL_A, COL_B, COL_C with your payload columns
--                        Keep SHA2() column list in sync with payload columns
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_SRC AS
SELECT
    -- !! UPDATE 4 OF 5 !! Composite key columns
    KEY_COL1,
    KEY_COL2,

    -- !! UPDATE 5 OF 5 !! Payload columns (non-key, non-audit)
    COL_A,
    COL_B,
    COL_C,
    -- Add more payload columns here, e.g.: COL_D, COL_E

    -- !! UPDATE 5 OF 5 !! Keep SHA2 column list in sync with payload columns above
    SHA2(CONCAT_WS('||',
        COALESCE(CAST(COL_A AS VARCHAR), ''),
        COALESCE(CAST(COL_B AS VARCHAR), ''),
        COALESCE(CAST(COL_C AS VARCHAR), '')
        -- Add more columns here, e.g.:
        -- ,COALESCE(CAST(COL_D AS VARCHAR), '')
    ), 256)                              AS RECORD_HASH,

    $BUSINESS_DATE::DATE                 AS STRT_DT

FROM SRC_TABLE                           -- !! UPDATE 2 OF 5 !!
WHERE $BUSINESS_DATE::DATE BETWEEN STRT_DT AND END_DT
  AND DELETED_FLAG = FALSE;


-- =============================================================================
-- STEP 2: PULL D-1 ACTIVE TARGET RECORDS INTO TMP_TGT_PREV
-- -----------------------------------------------------------------------------
-- Reads TGT_TABLE for records that were active on BUSINESS_DATE - 1.
-- This is the pre-load baseline — used to determine what should have changed.
--
-- NOTE: Since SCD2 has already run, rows that were updated/deleted will now
-- have END_DT = BUSINESS_DATE - 1 (set during the load). Querying for
-- BUSINESS_DATE - 1 BETWEEN STRT_DT AND END_DT still correctly returns them
-- because their END_DT was set to exactly that date by the SCD2 load.
-- This is why no explicit pre-load snapshot is needed.
--
-- !! UPDATE 3 OF 5 !! -- Replace TGT_TABLE with your actual target table name
-- !! UPDATE 4 OF 5 !! -- Replace KEY_COL1, KEY_COL2 with your composite key cols
-- !! UPDATE 5 OF 5 !! -- Replace COL_A, COL_B, COL_C with your payload columns
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_TGT_PREV AS
SELECT
    -- !! UPDATE 4 OF 5 !! Composite key columns
    KEY_COL1,
    KEY_COL2,

    -- !! UPDATE 5 OF 5 !! Payload columns (must match STEP 1 list)
    COL_A,
    COL_B,
    COL_C,
    -- Add more payload columns here to match STEP 1

    RECORD_HASH,
    STRT_DT,
    END_DT,
    SK,
    DELETED_FLAG

FROM TGT_TABLE T                         -- !! UPDATE 3 OF 5 !!
WHERE DATEADD(DAY, -1, $BUSINESS_DATE::DATE) BETWEEN T.STRT_DT AND T.END_DT
  AND T.DELETED_FLAG = FALSE
  AND NOT EXISTS (
      -- FIX: exclude keys that already have an active delete marker from a prior run.
      -- Without this, a record deleted on D-1 (END_DT=D-1 on its expired row)
      -- would incorrectly appear in D-1 snapshot and be classified as UPDATE
      -- instead of INSERT when it re-arrives in source today.
      SELECT 1
      FROM TGT_TABLE T2
      WHERE T2.KEY_COL1    = T.KEY_COL1   -- !! UPDATE 4 OF 5 !! match your key cols
        AND T2.KEY_COL2    = T.KEY_COL2
        AND T2.DELETED_FLAG = TRUE
        AND T2.END_DT       = '9999-12-31' -- active delete marker = this key is currently deleted
  );


-- =============================================================================
-- STEP 3: CLASSIFY RECORDS AS INSERT / UPDATE / DELETE INTO TMP_CLASSIFIED
-- -----------------------------------------------------------------------------
-- Determines what the SCD2 load SHOULD have done based on source vs D-1 target.
-- No changes needed here — driven entirely by TMP_SRC and TMP_TGT_PREV.
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_CLASSIFIED AS

-- INSERT: in source today, not in target D-1
SELECT
    S.KEY_COL1, S.KEY_COL2,
    S.COL_A, S.COL_B, S.COL_C,
    S.RECORD_HASH, S.STRT_DT,
    'INSERT' AS CHANGE_TYPE
FROM TMP_SRC S
LEFT JOIN TMP_TGT_PREV T
    ON  S.KEY_COL1 = T.KEY_COL1
    AND S.KEY_COL2 = T.KEY_COL2
WHERE T.KEY_COL1 IS NULL

UNION ALL

-- UPDATE: in both, but hash changed
SELECT
    S.KEY_COL1, S.KEY_COL2,
    S.COL_A, S.COL_B, S.COL_C,
    S.RECORD_HASH, S.STRT_DT,
    'UPDATE' AS CHANGE_TYPE
FROM TMP_SRC S
INNER JOIN TMP_TGT_PREV T
    ON  S.KEY_COL1 = T.KEY_COL1
    AND S.KEY_COL2 = T.KEY_COL2
WHERE S.RECORD_HASH <> T.RECORD_HASH

UNION ALL

-- DELETE: in target D-1, absent from source today
SELECT
    T.KEY_COL1, T.KEY_COL2,
    T.COL_A, T.COL_B, T.COL_C,
    T.RECORD_HASH,
    $BUSINESS_DATE::DATE AS STRT_DT,
    'DELETE' AS CHANGE_TYPE
FROM TMP_TGT_PREV T
LEFT JOIN TMP_SRC S
    ON  T.KEY_COL1 = S.KEY_COL1
    AND T.KEY_COL2 = S.KEY_COL2
WHERE S.KEY_COL1 IS NULL;


-- =============================================================================
-- STEP 4: SIMULATE EXPECTED SCD2 OUTPUT INTO TMP_EXPECTED
-- -----------------------------------------------------------------------------
-- Builds the complete set of rows that SHOULD exist in TGT_TABLE
-- for BUSINESS_DATE after a correct SCD2 load.
-- No writes to TGT_TABLE — this is a simulation only.
--
-- TMP_EXPECTED contains 3 row types:
--   a) NO CHANGE rows : active in D-1, still active today (hash unchanged)
--      → should still exist in target with END_DT = '9999-12-31'
--   b) NEW/UPDATED rows : INSERT and UPDATE classified rows
--      → should exist as new active rows with STRT_DT = BUSINESS_DATE
--   c) DELETE marker rows : DELETE classified rows
--      → should exist as DELETED_FLAG = TRUE rows with STRT_DT = BUSINESS_DATE
--
-- NOTE: Expired versions (old rows with END_DT = BUSINESS_DATE - 1) are
--       validated separately in STEP 5 via the EXPIRY check.
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_EXPECTED AS

-- a) NO CHANGE: records that exist in D-1 target and arrived unchanged today
--    These should still be active in target (END_DT = '9999-12-31')
SELECT
    T.KEY_COL1, T.KEY_COL2,
    T.COL_A, T.COL_B, T.COL_C,
    T.RECORD_HASH,
    T.STRT_DT,                           -- original start date preserved
    '9999-12-31'::DATE  AS END_DT,
    FALSE               AS DELETED_FLAG,
    'NO_CHANGE'         AS EXPECTED_TYPE
FROM TMP_TGT_PREV T
INNER JOIN TMP_SRC S
    ON  T.KEY_COL1 = S.KEY_COL1
    AND T.KEY_COL2 = S.KEY_COL2
WHERE T.RECORD_HASH = S.RECORD_HASH      -- hash unchanged = no new version needed

UNION ALL

-- b) NEW ACTIVE ROWS: INSERT and UPDATE cases
--    New version should exist in target with STRT_DT = BUSINESS_DATE
SELECT
    KEY_COL1, KEY_COL2,
    COL_A, COL_B, COL_C,
    RECORD_HASH,
    STRT_DT,                             -- = BUSINESS_DATE
    '9999-12-31'::DATE  AS END_DT,
    FALSE               AS DELETED_FLAG,
    CHANGE_TYPE         AS EXPECTED_TYPE
FROM TMP_CLASSIFIED
WHERE CHANGE_TYPE IN ('INSERT', 'UPDATE')

UNION ALL

-- c) DELETE MARKER ROWS: DELETE cases
--    Soft-delete row should exist in target with DELETED_FLAG = TRUE
SELECT
    KEY_COL1, KEY_COL2,
    COL_A, COL_B, COL_C,
    RECORD_HASH,
    STRT_DT,                             -- = BUSINESS_DATE
    '9999-12-31'::DATE  AS END_DT,
    TRUE                AS DELETED_FLAG,
    'DELETE_MARKER'     AS EXPECTED_TYPE
FROM TMP_CLASSIFIED
WHERE CHANGE_TYPE = 'DELETE';


-- =============================================================================
-- STEP 5: COMPARE TMP_EXPECTED VS TGT_TABLE
-- -----------------------------------------------------------------------------
-- Two MINUS checks per scenario:
--   MISSING_FROM_TARGET : in TMP_EXPECTED but not in TGT_TABLE → load missed rows
--   EXTRA_IN_TARGET     : in TGT_TABLE but not in TMP_EXPECTED → load wrote extra rows
--
-- Additionally:
--   EXPIRY_CHECK : verifies that old versions were correctly expired
--                  (END_DT set to BUSINESS_DATE - 1 for UPDATE and DELETE cases)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- CHECK A: MISSING_FROM_TARGET
-- Rows the load SHOULD have written but are absent from TGT_TABLE
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_MISSING AS
SELECT
    E.KEY_COL1,
    E.KEY_COL2,
    E.RECORD_HASH,
    E.STRT_DT,
    E.END_DT,
    E.DELETED_FLAG,
    E.EXPECTED_TYPE,
    'MISSING_FROM_TARGET' AS ISSUE_TYPE
FROM (
    -- What should be in target
    SELECT KEY_COL1, KEY_COL2, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG, EXPECTED_TYPE
    FROM TMP_EXPECTED

    MINUS

    -- What is actually in target (active and delete-marker rows for business date)
    SELECT KEY_COL1, KEY_COL2, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG,
           NULL AS EXPECTED_TYPE         -- target has no EXPECTED_TYPE column
    FROM TGT_TABLE                       -- !! UPDATE 3 OF 5 !!
    WHERE $BUSINESS_DATE::DATE BETWEEN STRT_DT AND END_DT
) E;


-- -----------------------------------------------------------------------------
-- CHECK B: EXTRA_IN_TARGET
-- Rows the load wrote to TGT_TABLE that were NOT expected by the SCD2 logic
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_EXTRA AS
SELECT
    KEY_COL1,
    KEY_COL2,
    RECORD_HASH,
    STRT_DT,
    END_DT,
    DELETED_FLAG,
    'EXTRA_IN_TARGET' AS ISSUE_TYPE
FROM (
    -- FIX: only check rows written by TODAY's load (STRT_DT = BUSINESS_DATE).
    -- Using BUSINESS_DATE BETWEEN STRT_DT AND END_DT would also catch rows from
    -- prior runs (e.g. a delete marker written yesterday that is still open-ended),
    -- incorrectly flagging them as extra rows from today's load.
    SELECT KEY_COL1, KEY_COL2, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG
    FROM TGT_TABLE                       -- !! UPDATE 3 OF 5 !!
    WHERE STRT_DT = $BUSINESS_DATE::DATE -- only rows actually written today

    MINUS

    -- What should have been written today
    SELECT KEY_COL1, KEY_COL2, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG
    FROM TMP_EXPECTED
    WHERE STRT_DT = $BUSINESS_DATE::DATE -- only today's expected writes
) X;


-- -----------------------------------------------------------------------------
-- CHECK C: EXPIRY_CHECK
-- For UPDATE and DELETE cases, the old active row should have been expired:
--   END_DT should = BUSINESS_DATE - 1
-- Finds UPDATE/DELETE keys whose prior version is NOT correctly expired in target
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_EXPIRY_ISSUES AS
SELECT
    C.KEY_COL1,
    C.KEY_COL2,
    C.CHANGE_TYPE,
    T.STRT_DT       AS OLD_STRT_DT,
    T.END_DT        AS ACTUAL_END_DT,
    DATEADD(DAY, -1, $BUSINESS_DATE::DATE) AS EXPECTED_END_DT,
    'EXPIRY_WRONG'  AS ISSUE_TYPE
FROM TMP_CLASSIFIED C
-- Find the prior version row in target (the one that should have been expired)
INNER JOIN TGT_TABLE T                   -- !! UPDATE 3 OF 5 !!
    ON  T.KEY_COL1 = C.KEY_COL1
    AND T.KEY_COL2 = C.KEY_COL2
    AND T.DELETED_FLAG = FALSE
    AND T.STRT_DT < $BUSINESS_DATE::DATE -- it's a prior version, not the new one
WHERE C.CHANGE_TYPE IN ('UPDATE', 'DELETE')
  -- The old row should have END_DT = BUSINESS_DATE - 1 after the load
  AND T.END_DT <> DATEADD(DAY, -1, $BUSINESS_DATE::DATE);


-- =============================================================================
-- STEP 6: VALIDATION SUMMARY
-- -----------------------------------------------------------------------------
-- Single result set showing PASS/FAIL for each check.
-- Expected: all rows showing DISCREPANCY_COUNT = 0 and STATUS = PASS.
-- Any FAIL row means the SCD2 load for this business date is incorrect.
-- Drill into TMP_MISSING, TMP_EXTRA, or TMP_EXPIRY_ISSUES for row-level detail.
-- =============================================================================
SELECT CHECK_TYPE, DISCREPANCY_COUNT, STATUS
FROM (

    SELECT
        'A: MISSING_FROM_TARGET'             AS CHECK_TYPE,
        COUNT(*)                             AS DISCREPANCY_COUNT,
        CASE WHEN COUNT(*) = 0
             THEN 'PASS' ELSE 'FAIL' END     AS STATUS
    FROM TMP_MISSING

    UNION ALL

    SELECT
        'B: EXTRA_IN_TARGET'                 AS CHECK_TYPE,
        COUNT(*)                             AS DISCREPANCY_COUNT,
        CASE WHEN COUNT(*) = 0
             THEN 'PASS' ELSE 'FAIL' END     AS STATUS
    FROM TMP_EXTRA

    UNION ALL

    SELECT
        'C: EXPIRY_CHECK'                    AS CHECK_TYPE,
        COUNT(*)                             AS DISCREPANCY_COUNT,
        CASE WHEN COUNT(*) = 0
             THEN 'PASS' ELSE 'FAIL' END     AS STATUS
    FROM TMP_EXPIRY_ISSUES

) SUMMARY
ORDER BY CHECK_TYPE;


-- =============================================================================
-- OPTIONAL: DRILL-DOWN QUERIES — run individually to investigate failures
-- =============================================================================

-- Rows expected in target but missing:
-- SELECT * FROM TMP_MISSING ORDER BY KEY_COL1, EXPECTED_TYPE;

-- Rows in target that were not expected:
-- SELECT * FROM TMP_EXTRA ORDER BY KEY_COL1;

-- Old versions not correctly expired:
-- SELECT * FROM TMP_EXPIRY_ISSUES ORDER BY KEY_COL1;