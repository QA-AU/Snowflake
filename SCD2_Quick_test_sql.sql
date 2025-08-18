SCD2_Quick_test_sql.py

/* ============================================================================
   SCD2 QUICK VALIDATOR
   Purpose:
     Given (1) a raw SOURCE table and (2) a TARGET table with SCD2 already
     applied, validate that SCD2 was applied correctly.

   Inputs (edit the SET lines below):
     - SRC_SCHEMA , SRC_TABLE    : the input data on which SCD2 was applied
     - TGT_SCHEMA , TGT_TABLE    : the post-applied SCD2 table
     - PK_LIST_TGT               : comma-separated PK column names in TARGET
     - PK_JOIN_ON                : join predicate between SRC (alias s) and
                                   TARGET (alias t) on the natural key(s)
     - USE_IS_CURRENT            : TRUE → use IS_CURRENT='Y' as "open"
                                   FALSE → use END_DATE = '9999-12-31' as "open"
     - HIGH_DATE                 : the high date used for open rows

   What it checks (all PASS/FAIL):
     1) No inverted ranges: FROM_DATE <= END_DATE
     2) Exactly one current row per PK (based on open predicate)
     3) No overlapping date ranges per PK
     4) Source coverage: every source PK maps to an open/current target row
     5) No orphan closed rows (closed row is followed by next row starting +1 day)
     6) Optional gap signal (non-fatal): gaps in date chains per PK

   Notes:
     - This script is read-only. It does not modify any data.
     - Uses Snowflake Scripting to print concise PASS/FAIL lines.
============================================================================ */

/* =======================
   EDIT THESE INPUTS ONLY
   ======================= */
SET SRC_SCHEMA     = 'SRC_STG';
SET SRC_TABLE      = 'CUSTOMER_STG';

SET TGT_SCHEMA     = 'TGT_DW';
SET TGT_TABLE      = 'DIM_CUSTOMER';

/* PKs in TARGET, comma-separated (e.g., CUSTOMER_ID or CUST_ID,BRAND_ID) */
SET PK_LIST_TGT    = 'CUSTOMER_ID';

/* Join predicate between s (SRC) and t (TARGET) using natural keys.
   Example (single key):  s.CUSTOMER_ID = t.CUSTOMER_ID
   Example (multi key) :  s.CUST_ID = t.CUST_ID AND s.BRAND_ID = t.BRAND_ID
*/
SET PK_JOIN_ON     = 's.CUSTOMER_ID = t.CUSTOMER_ID';

/* TRUE → use IS_CURRENT to identify open rows; FALSE → use END_DATE = HIGH_DATE */
SET USE_IS_CURRENT = TRUE;

/* High date used for open rows when using END_DATE */
SET HIGH_DATE      = '9999-12-31';


/* =======================================================================
   DO NOT EDIT BELOW THIS LINE
   ======================================================================= */
BEGIN
  LET src_fqn STRING := UPPER($SRC_SCHEMA)||'.'||UPPER($SRC_TABLE);
  LET tgt_fqn STRING := UPPER($TGT_SCHEMA)||'.'||UPPER($TGT_TABLE);
  LET pk_csv  STRING := $PK_LIST_TGT;            -- e.g. CUSTOMER_ID or C1,C2
  LET hi_date STRING := $HIGH_DATE;

  /* Build open predicate */
  LET open_pred STRING;
  IF $USE_IS_CURRENT THEN
    open_pred := 'UPPER(IS_CURRENT) = ''Y''';
  ELSE
    open_pred := 'END_DATE = DATE '''||hi_date||'''';
  END IF;

  /* ---------- 1) Inverted ranges (FROM_DATE > END_DATE) ---------- */
  LET c_inverted NUMBER := 0;
  EXECUTE IMMEDIATE
    'SELECT COUNT(*) FROM '||tgt_fqn||' WHERE FROM_DATE > END_DATE'
  INTO :c_inverted;

  /* ---------- 2) Exactly one current row per PK ---------- */
  LET c_current_bad NUMBER := 0;
  EXECUTE IMMEDIATE
    'SELECT COUNT(*) FROM ('||
    '  SELECT '||pk_csv||', COUNT(*) AS c'||
    '  FROM '||tgt_fqn||
    '  WHERE '||open_pred||
    '  GROUP BY '||pk_csv||
    '  HAVING COUNT(*) <> 1'||
    ')'
  INTO :c_current_bad;

  /* ---------- 3) No overlapping ranges per PK ----------
     Overlap if two ranges for same PK intersect in time.
     Pairwise check using self-join.
  */
  LET c_overlap NUMBER := 0;
  EXECUTE IMMEDIATE
    'SELECT COUNT(*) FROM ('||
    '  SELECT 1'||
    '  FROM '||tgt_fqn||' a JOIN '||tgt_fqn||' b'||
    '    ON ('|| LISTAGG('a.'||TRIM(col)||'=b.'||TRIM(col), ' AND ')
                   WITHIN GROUP (ORDER BY col)
        FROM TABLE(SPLIT_TO_TABLE(pk_csv,',')) ) /* inlined via dynamic below */'
  INTO :c_overlap;  -- <-- we’ll rebuild properly with dynamic below

  /* Rebuild 3) with explicit dynamic using parsed PK list */
  LET pk_cond STRING := '';
  FOR rec IN (SELECT TRIM(value) AS col FROM TABLE(SPLIT_TO_TABLE(pk_csv, ',')))
  DO
    IF pk_cond IS NULL OR pk_cond = '' THEN
      pk_cond := 'a.'||rec.col||' = b.'||rec.col;
    ELSE
      pk_cond := pk_cond||' AND a.'||rec.col||' = b.'||rec.col;
    END IF;
  END FOR;

  LET sql_overlap STRING := 'SELECT COUNT(*) FROM ('||
    ' SELECT 1'||
    ' FROM '||tgt_fqn||' a JOIN '||tgt_fqn||' b'||
    '   ON '||pk_cond||
    '  AND (a.FROM_DATE <= b.END_DATE AND b.FROM_DATE <= a.END_DATE)'||
    '  AND (a.ROWID <> b.ROWID)'||
    ')';
  EXECUTE IMMEDIATE :sql_overlap INTO :c_overlap;

  /* ---------- 4) Source coverage: every SRC PK appears in current TARGET ---------- */
  LET c_coverage_miss NUMBER := 0;
  LET sql_cov STRING := 'SELECT COUNT(*) FROM '||src_fqn||' s '||
    'LEFT JOIN '||tgt_fqn||' t ON '||$PK_JOIN_ON||' AND ('||open_pred||') '||
    'WHERE '||
    CASE
      WHEN POSITION(' AND ' IN $PK_JOIN_ON) = 0 THEN 't.'||SPLIT_PART($PK_JOIN_ON, ' = ', 2)||' IS NULL'
      ELSE
        /* For multi-PK, check any target PK is NULL after join */
        LISTAGG('t.'||TRIM(value)||' IS NULL',' OR ')
          WITHIN GROUP (ORDER BY value)
          FROM TABLE(SPLIT_TO_TABLE(pk_csv, ','))
    END;
  /* The dynamic above is not allowed inline; build it procedurally: */
  LET tgt_null_pred STRING := '';
  FOR rec2 IN (SELECT TRIM(value) AS col FROM TABLE(SPLIT_TO_TABLE(pk_csv, ',')))
  DO
    IF tgt_null_pred IS NULL OR tgt_null_pred = '' THEN
      tgt_null_pred := 't.'||rec2.col||' IS NULL';
    ELSE
      tgt_null_pred := tgt_null_pred||' OR t.'||rec2.col||' IS NULL';
    END IF;
  END FOR;
  sql_cov := 'SELECT COUNT(*) FROM '||src_fqn||' s '||
             'LEFT JOIN '||tgt_fqn||' t ON '||$PK_JOIN_ON||' AND ('||open_pred||') '||
             'WHERE '||tgt_null_pred;
  EXECUTE IMMEDIATE :sql_cov INTO :c_coverage_miss;

  /* ---------- 5) No orphan closed rows ----------
     Every closed row should be immediately followed by a next row whose
     FROM_DATE = END_DATE + 1 for the same PK (unless business allows gaps).
  */
  LET c_orphan NUMBER := 0;
  LET sql_orphan STRING :=
    'WITH x AS ('||
    '  SELECT '||pk_csv||', FROM_DATE, END_DATE,'||
    '         LEAD(FROM_DATE) OVER (PARTITION BY '||pk_csv||' ORDER BY FROM_DATE) AS next_from'||
    '  FROM '||tgt_fqn||
    ')'||
    ' SELECT COUNT(*) FROM x'||
    ' WHERE END_DATE <> DATE '''||hi_date||''' '||
    '   AND (next_from IS NULL OR next_from <> DATEADD(DAY,1,END_DATE))';
  EXECUTE IMMEDIATE :sql_orphan INTO :c_orphan;

  /* ---------- 6) Optional: gaps in date chains (non-fatal) ---------- */
  LET c_gaps NUMBER := 0;
  LET sql_gaps STRING :=
    'WITH x AS ('||
    '  SELECT '||pk_csv||', FROM_DATE, END_DATE,'||
    '         LEAD(FROM_DATE) OVER (PARTITION BY '||pk_csv||' ORDER BY FROM_DATE) AS next_from'||
    '  FROM '||tgt_fqn||
    ')'||
    ' SELECT COUNT(*) FROM x'||
    ' WHERE END_DATE <> DATE '''||hi_date||''' '||
    '   AND next_from IS NOT NULL AND next_from <> DATEADD(DAY,1,END_DATE)';
  EXECUTE IMMEDIATE :sql_gaps INTO :c_gaps;

  /* ---------- Print PASS/FAIL summary ---------- */
  SELECT 'SCD2 VALIDATION SUMMARY' AS SECTION,
         :src_fqn AS SRC,
         :tgt_fqn AS TARGET,
         :pk_csv  AS PKS,
         :open_pred AS OPEN_CRITERION;

  SELECT '1) Inverted ranges (FROM_DATE > END_DATE)' AS CHECK_NAME,
         :c_inverted AS VIOLATIONS,
         IFF(:c_inverted=0,'PASS','FAIL') AS STATUS;

  SELECT '2) Exactly one current row per PK' AS CHECK_NAME,
         :c_current_bad AS VIOLATIONS,
         IFF(:c_current_bad=0,'PASS','FAIL') AS STATUS;

  SELECT '3) Overlapping date ranges per PK' AS CHECK_NAME,
         :c_overlap AS VIOLATIONS,
         IFF(:c_overlap=0,'PASS','FAIL') AS STATUS;

  SELECT '4) Source coverage missing in current target' AS CHECK_NAME,
         :c_coverage_miss AS VIOLATIONS,
         IFF(:c_coverage_miss=0,'PASS','FAIL') AS STATUS;

  SELECT '5) Orphan closed rows (no next-from_date = end_date+1)' AS CHECK_NAME,
         :c_orphan AS VIOLATIONS,
         IFF(:c_orphan=0,'PASS','FAIL') AS STATUS;

  SELECT '6) Gaps in date chains (non-fatal signal)' AS CHECK_NAME,
         :c_gaps AS ROWS_WITH_GAPS,
         IFF(:c_gaps=0,'PASS*','WARN') AS STATUS;

  /* Overall PASS/FAIL (strict on 1–5; 6 is informational) */
  LET overall_ok BOOLEAN := (:c_inverted=0 AND :c_current_bad=0 AND :c_overlap=0 AND :c_coverage_miss=0 AND :c_orphan=0);
  SELECT IFF(:overall_ok,'✅ VALIDATION PASSED','❌ VALIDATION FAILED') AS OVERALL_STATUS;

END;
