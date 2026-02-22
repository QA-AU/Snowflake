scd2.py
-- =============================================================================
-- SCD2 TEST HARNESS — SNOWFLAKE
-- =============================================================================
-- PURPOSE : Validates the SCD2 processing logic using mock data.
--           Run this entire script top to bottom in a Snowflake worksheet.
--           Check STEP T6 for final PASS/FAIL on all test cases.
--
-- IMPORTANT DESIGN NOTE:
--   TMP_TGT_PREV is snapshotted BEFORE any writes to TGT_TABLE.
--   After Steps 4a/4b/4c run, TGT_TABLE is modified and the pre-load state
--   is no longer directly accessible. All validations therefore use either:
--     - TMP_SRC       : today's source (unchanged throughout)
--     - TMP_TGT_PREV  : pre-load snapshot of D-1 active target rows
--     - TGT_TABLE     : post-load state (what was written)
--   No validation assumes knowledge of what individual column values
--   looked like before the load.
--
-- TEST CASES COVERED:
--   TC-01 : NO CHANGE   — same key+hash in source and D-1 target → no new row
--   TC-02 : INSERT      — key in source, absent in D-1 target    → new active row
--   TC-03 : UPDATE      — key in both, hash changed              → expire old, insert new
--   TC-04 : DELETE      — key in D-1 target, absent from source  → expire old, insert delete marker
--   TC-05 : RE-ACTIVATE — key had a prior deleted row, arrives again → new active row
--
-- EXPECTED FINAL RESULT (Step T6):
--   INSERT_RECON  → DISCREPANCY_COUNT = 0, STATUS = PASS
--   UPDATE_RECON  → DISCREPANCY_COUNT = 0, STATUS = PASS
--   DELETE_RECON  → DISCREPANCY_COUNT = 0, STATUS = PASS
-- =============================================================================


-- =============================================================================
-- SET TEST BUSINESS DATE
-- =============================================================================
SET BUSINESS_DATE = '2026-02-21';    -- D   = today being processed
                                     -- D-1 = 2026-02-20 (yesterday baseline)


-- =============================================================================
-- STEP T1: CREATE MOCK SOURCE TABLE (SRC_TABLE)
-- -----------------------------------------------------------------------------
-- Simulates records arriving in source on BUSINESS_DATE.
--   K1 : NO CHANGE  — same payload as yesterday
--   K2 : INSERT     — brand new, never seen before
--   K3 : UPDATE     — existed yesterday, payload changed
--   K5 : RE-ACTIVATE — was previously deleted, arriving again
-- NOTE: K4 intentionally absent = DELETE case
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE SRC_TABLE AS
SELECT * FROM VALUES
    ('K1', 'K1', 'Alpha',     100, 'X', '2026-01-01'::DATE, '9999-12-31'::DATE, FALSE),
    ('K2', 'K2', 'Beta',      200, 'Y', '2026-02-21'::DATE, '9999-12-31'::DATE, FALSE),
    ('K3', 'K3', 'Gamma_NEW', 300, 'Z', '2026-02-21'::DATE, '9999-12-31'::DATE, FALSE),
    ('K5', 'K5', 'Epsilon',   500, 'E', '2026-02-21'::DATE, '9999-12-31'::DATE, FALSE)
AS t(KEY_COL1, KEY_COL2, COL_A, COL_B, COL_C, STRT_DT, END_DT, DELETED_FLAG);


-- =============================================================================
-- STEP T2: CREATE MOCK TARGET TABLE (TGT_TABLE)
-- -----------------------------------------------------------------------------
-- Simulates the SCD2 target BEFORE today's load.
-- Reflects what was active as of D-1 (2026-02-20):
--   K1 : active, same payload as source today       (TC-01: no change)
--   K3 : active, OLD payload                        (TC-03: will be updated)
--   K4 : active                                     (TC-04: will be deleted)
--   K5 : has a prior deleted row (END_DT in past)   (TC-05: re-activate)
-- NOTE: K2 does not exist yet                       (TC-02: fresh insert)
--
-- RECORD_HASH values here must be pre-computed to match SHA2 of payload cols.
-- For test purposes we use placeholder strings — in production these would be
-- real SHA2 hashes. The UPDATE detection relies on hash mismatch between
-- TMP_SRC (computed fresh) and TMP_TGT_PREV (stored hash from target).
-- For TC-01 (no change): source hash must equal target hash → we pre-align them.
-- For TC-03 (update):    source hash will differ            → mismatch triggers update.
-- =============================================================================

-- Pre-compute hashes for mock data alignment
SET HASH_K1 = (SELECT SHA2(CONCAT_WS('||', 'Alpha', '100', 'X'), 256));  -- matches source K1
SET HASH_K3_OLD = (SELECT SHA2(CONCAT_WS('||', 'Gamma_OLD', '300', 'Z'), 256));  -- old payload, will differ from source
SET HASH_K4 = (SELECT SHA2(CONCAT_WS('||', 'Delta', '400', 'D'), 256));
SET HASH_K5 = (SELECT SHA2(CONCAT_WS('||', 'Epsilon', '500', 'E'), 256));

CREATE OR REPLACE TEMPORARY TABLE TGT_TABLE AS
SELECT * FROM VALUES
    ('SK-K1',  'K1', 'K1', 'Alpha',     100, 'X', $HASH_K1,     '2026-01-01'::DATE, '9999-12-31'::DATE, FALSE),
    ('SK-K3',  'K3', 'K3', 'Gamma_OLD', 300, 'Z', $HASH_K3_OLD, '2026-01-01'::DATE, '9999-12-31'::DATE, FALSE),
    ('SK-K4',  'K4', 'K4', 'Delta',     400, 'D', $HASH_K4,     '2026-01-01'::DATE, '9999-12-31'::DATE, FALSE),
    ('SK-K5D', 'K5', 'K5', 'Epsilon',   500, 'E', $HASH_K5,     '2026-01-01'::DATE, '2026-01-31'::DATE, TRUE)
AS t(SK, KEY_COL1, KEY_COL2, COL_A, COL_B, COL_C, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG);


-- =============================================================================
-- STEP T3: BUILD TMP_SRC (identical to production)
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_SRC AS
SELECT
    KEY_COL1,
    KEY_COL2,
    COL_A,
    COL_B,
    COL_C,
    SHA2(CONCAT_WS('||',
        COALESCE(CAST(COL_A AS VARCHAR), ''),
        COALESCE(CAST(COL_B AS VARCHAR), ''),
        COALESCE(CAST(COL_C AS VARCHAR), '')
    ), 256)              AS RECORD_HASH,
    $BUSINESS_DATE::DATE AS STRT_DT
FROM SRC_TABLE
WHERE $BUSINESS_DATE::DATE BETWEEN STRT_DT AND END_DT
  AND DELETED_FLAG = FALSE;

SELECT 'TMP_SRC row count (expect 4: K1 K2 K3 K5)' AS CHECK, COUNT(*) AS ACTUAL FROM TMP_SRC;


-- =============================================================================
-- STEP T4: BUILD TMP_TGT_PREV (identical to production)
-- *** SNAPSHOTTED HERE — BEFORE ANY WRITES TO TGT_TABLE ***
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_TGT_PREV AS
SELECT
    KEY_COL1,
    KEY_COL2,
    COL_A,
    COL_B,
    COL_C,
    RECORD_HASH,
    STRT_DT,
    END_DT,
    SK,
    DELETED_FLAG
FROM TGT_TABLE
WHERE DATEADD(DAY, -1, $BUSINESS_DATE::DATE) BETWEEN STRT_DT AND END_DT
  AND DELETED_FLAG = FALSE;

SELECT 'TMP_TGT_PREV row count (expect 3: K1 K3 K4 — K5 was deleted, K2 never existed)' AS CHECK, COUNT(*) AS ACTUAL FROM TMP_TGT_PREV;


-- =============================================================================
-- STEP T5: BUILD TMP_CLASSIFIED (identical to production)
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE TMP_CLASSIFIED AS

SELECT S.KEY_COL1, S.KEY_COL2, S.COL_A, S.COL_B, S.COL_C,
       S.RECORD_HASH, S.STRT_DT, 'INSERT' AS CHANGE_TYPE
FROM TMP_SRC S
LEFT JOIN TMP_TGT_PREV T ON S.KEY_COL1 = T.KEY_COL1 AND S.KEY_COL2 = T.KEY_COL2
WHERE T.KEY_COL1 IS NULL

UNION ALL

SELECT S.KEY_COL1, S.KEY_COL2, S.COL_A, S.COL_B, S.COL_C,
       S.RECORD_HASH, S.STRT_DT, 'UPDATE' AS CHANGE_TYPE
FROM TMP_SRC S
INNER JOIN TMP_TGT_PREV T ON S.KEY_COL1 = T.KEY_COL1 AND S.KEY_COL2 = T.KEY_COL2
WHERE S.RECORD_HASH <> T.RECORD_HASH

UNION ALL

SELECT T.KEY_COL1, T.KEY_COL2, T.COL_A, T.COL_B, T.COL_C,
       T.RECORD_HASH, $BUSINESS_DATE::DATE AS STRT_DT, 'DELETE' AS CHANGE_TYPE
FROM TMP_TGT_PREV T
LEFT JOIN TMP_SRC S ON T.KEY_COL1 = S.KEY_COL1 AND T.KEY_COL2 = S.KEY_COL2
WHERE S.KEY_COL1 IS NULL;

-- Verify classification using TMP_SRC and TMP_TGT_PREV only (no prior state assumed)
SELECT 'TMP_CLASSIFIED (expect K2=INSERT, K3=UPDATE, K4=DELETE, K5=INSERT)' AS CHECK;
SELECT KEY_COL1, KEY_COL2, CHANGE_TYPE FROM TMP_CLASSIFIED ORDER BY CHANGE_TYPE, KEY_COL1;
SELECT 'TMP_CLASSIFIED total (expect 4, K1 must NOT appear)' AS CHECK, COUNT(*) AS ACTUAL FROM TMP_CLASSIFIED;

-- Explicit check: K1 must NOT be classified (no change)
SELECT 'TC-01 NO CHANGE check — K1 must not appear in classified (expect 0)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TMP_CLASSIFIED WHERE KEY_COL1 = 'K1';


-- =============================================================================
-- STEP T5b: APPLY SCD2 TO TGT_TABLE (identical to production)
-- =============================================================================

-- 4a: Expire old active rows
UPDATE TGT_TABLE
SET    END_DT = DATEADD(DAY, -1, C.STRT_DT)
FROM   TMP_CLASSIFIED C
WHERE  TGT_TABLE.KEY_COL1    = C.KEY_COL1
  AND  TGT_TABLE.KEY_COL2    = C.KEY_COL2
  AND  TGT_TABLE.END_DT      = '9999-12-31'
  AND  TGT_TABLE.DELETED_FLAG = FALSE
  AND  C.CHANGE_TYPE IN ('UPDATE', 'DELETE');

-- 4b: Insert new active rows
INSERT INTO TGT_TABLE (SK, KEY_COL1, KEY_COL2, COL_A, COL_B, COL_C, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG)
SELECT
    SHA2(CONCAT_WS('||',
        COALESCE(CAST(KEY_COL1 AS VARCHAR),''),
        COALESCE(CAST(KEY_COL2 AS VARCHAR),''),
        CAST(STRT_DT AS VARCHAR)
    ), 256),
    KEY_COL1, KEY_COL2, COL_A, COL_B, COL_C, RECORD_HASH,
    STRT_DT, '9999-12-31'::DATE, FALSE
FROM TMP_CLASSIFIED WHERE CHANGE_TYPE IN ('INSERT', 'UPDATE');

-- 4c: Insert soft-delete markers
INSERT INTO TGT_TABLE (SK, KEY_COL1, KEY_COL2, COL_A, COL_B, COL_C, RECORD_HASH, STRT_DT, END_DT, DELETED_FLAG)
SELECT
    SHA2(CONCAT_WS('||',
        COALESCE(CAST(KEY_COL1 AS VARCHAR),''),
        COALESCE(CAST(KEY_COL2 AS VARCHAR),''),
        CAST(STRT_DT AS VARCHAR),
        'DELETED'
    ), 256),
    KEY_COL1, KEY_COL2, COL_A, COL_B, COL_C, RECORD_HASH,
    STRT_DT, '9999-12-31'::DATE, TRUE
FROM TMP_CLASSIFIED WHERE CHANGE_TYPE = 'DELETE';


-- =============================================================================
-- STEP T5c: POST-LOAD STRUCTURAL VALIDATIONS
-- -----------------------------------------------------------------------------
-- All checks use ONLY post-load TGT_TABLE, TMP_SRC, and TMP_TGT_PREV.
-- No hardcoded "before" column values assumed — we validate structure and
-- counts, not specific payload values we no longer have access to.
-- =============================================================================

-- TC-01: NO CHANGE — K1 should still have exactly 1 active row, unchanged
SELECT 'TC-01 NO CHANGE: K1 active row count (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K1' AND END_DT = '9999-12-31' AND DELETED_FLAG = FALSE;

-- TC-02: INSERT — K2 should now have exactly 1 active row in target
SELECT 'TC-02 INSERT: K2 active row count (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K2' AND END_DT = '9999-12-31' AND DELETED_FLAG = FALSE;

-- TC-03: UPDATE — K3 should have 1 expired row and 1 new active row
SELECT 'TC-03 UPDATE: K3 expired row count (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K3' AND END_DT = DATEADD(DAY, -1, $BUSINESS_DATE::DATE) AND DELETED_FLAG = FALSE;

SELECT 'TC-03 UPDATE: K3 active row count (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K3' AND END_DT = '9999-12-31' AND DELETED_FLAG = FALSE;

-- TC-03: new active row hash must match source hash (confirms payload was written correctly)
SELECT 'TC-03 UPDATE: K3 hash matches source (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE T
INNER JOIN TMP_SRC S ON T.KEY_COL1 = S.KEY_COL1 AND T.KEY_COL2 = S.KEY_COL2
WHERE T.KEY_COL1 = 'K3'
  AND T.RECORD_HASH = S.RECORD_HASH
  AND T.END_DT = '9999-12-31'
  AND T.DELETED_FLAG = FALSE;

-- TC-04: DELETE — K4 should have 1 expired non-deleted row and 1 delete marker
SELECT 'TC-04 DELETE: K4 expired non-deleted row (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K4' AND END_DT = DATEADD(DAY, -1, $BUSINESS_DATE::DATE) AND DELETED_FLAG = FALSE;

SELECT 'TC-04 DELETE: K4 delete marker (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K4' AND END_DT = '9999-12-31' AND DELETED_FLAG = TRUE;

-- TC-05: RE-ACTIVATE — K5 should have 1 prior deleted row (preserved) and 1 new active row
SELECT 'TC-05 RE-ACTIVATE: K5 prior deleted row preserved (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K5' AND DELETED_FLAG = TRUE AND END_DT <> '9999-12-31';

SELECT 'TC-05 RE-ACTIVATE: K5 new active row (expect 1)' AS CHECK,
       COUNT(*) AS ACTUAL
FROM TGT_TABLE
WHERE KEY_COL1 = 'K5' AND END_DT = '9999-12-31' AND DELETED_FLAG = FALSE;


-- =============================================================================
-- STEP T6: RECONCILIATION SUMMARY — FINAL PASS/FAIL
-- All three must return DISCREPANCY_COUNT = 0 and STATUS = PASS
-- =============================================================================
SELECT CHECK_TYPE, DISCREPANCY_COUNT, STATUS
FROM (

    SELECT 'INSERT_RECON'                    AS CHECK_TYPE,
           COUNT(*)                          AS DISCREPANCY_COUNT,
           CASE WHEN COUNT(*) = 0
                THEN 'PASS' ELSE 'FAIL' END  AS STATUS
    FROM (
        SELECT KEY_COL1, KEY_COL2, RECORD_HASH FROM TMP_SRC
        MINUS
        SELECT KEY_COL1, KEY_COL2, RECORD_HASH FROM TGT_TABLE
        WHERE  $BUSINESS_DATE::DATE BETWEEN STRT_DT AND END_DT
          AND  DELETED_FLAG = FALSE
    ) A

    UNION ALL

    SELECT 'UPDATE_RECON'                    AS CHECK_TYPE,
           COUNT(*)                          AS DISCREPANCY_COUNT,
           CASE WHEN COUNT(*) = 0
                THEN 'PASS' ELSE 'FAIL' END  AS STATUS
    FROM (
        SELECT KEY_COL1, KEY_COL2, RECORD_HASH FROM TMP_SRC
        MINUS
        SELECT KEY_COL1, KEY_COL2, RECORD_HASH FROM TGT_TABLE
        WHERE  $BUSINESS_DATE::DATE BETWEEN STRT_DT AND END_DT
          AND  DELETED_FLAG = FALSE
    ) B
    INNER JOIN TMP_TGT_PREV P
        ON  B.KEY_COL1 = P.KEY_COL1
        AND B.KEY_COL2 = P.KEY_COL2

    UNION ALL

    SELECT 'DELETE_RECON'                    AS CHECK_TYPE,
           COUNT(*)                          AS DISCREPANCY_COUNT,
           CASE WHEN COUNT(*) = 0
                THEN 'PASS' ELSE 'FAIL' END  AS STATUS
    FROM (
        SELECT KEY_COL1, KEY_COL2 FROM TMP_TGT_PREV
        MINUS
        SELECT KEY_COL1, KEY_COL2 FROM TMP_SRC
    ) EXPECTED_DEL
    LEFT JOIN TGT_TABLE T
        ON  EXPECTED_DEL.KEY_COL1 = T.KEY_COL1
        AND EXPECTED_DEL.KEY_COL2 = T.KEY_COL2
        AND T.DELETED_FLAG = TRUE
        AND $BUSINESS_DATE::DATE BETWEEN T.STRT_DT AND T.END_DT
    WHERE T.KEY_COL1 IS NULL

) SUMMARY
ORDER BY CHECK_TYPE;