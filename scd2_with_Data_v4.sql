scd2_with_Data_v4.py

-- ============================================
-- SRC1 SCD2 VALIDATION (INSERT / UPDATE / DELETE)
-- Tables:
--   Source            : QA_SRC_1           (ID, NAME, EMAIL, EXTRACT_DATE, IS_DELETED)
--   Pre SCD2 (snapshot): QA_PRE_SCD2       (ID, NAME, EMAIL, START_DT, END_DT, CURRENT_FLG, IS_DELETED, SRC_SYS_CD)
--   Post SCD2 (actual) : QA_POST_SCD2      (same cols as PRE)
-- ============================================

-- ====== PARAMETERS ======
SET LOAD_DATE = '2025-01-07'::DATE;  -- change per run
-- If you want to switch to NULL open-end, replace DATE '9999-12-31' with NULL in EXPECTED rows below.

-- =====================================================================
-- 1) INSERT VALIDATION — pick a true insert (never an update), then show
--    SRC (today), PRE (none/empty), EXPECTED_POST, POST (actual current)
-- =====================================================================
WITH
post_new AS (
  SELECT ID, NAME
  FROM   QA_POST_SCD2
  WHERE  START_DT = $LOAD_DATE
    AND  CURRENT_FLG = TRUE
    AND  COALESCE(IS_DELETED, 0) = 0
    AND  SRC_SYS_CD = 'SRC_1'
),
pick_inserts AS (
  SELECT pn.ID, pn.NAME
  FROM   post_new pn
  LEFT JOIN QA_PRE_SCD2 pr
         ON pr.ID = pn.ID AND pr.NAME = pn.NAME
  WHERE  pr.ID IS NULL
    AND  NOT EXISTS (
           SELECT 1
           FROM   QA_POST_SCD2 x
           WHERE  x.ID = pn.ID
             AND  x.NAME = pn.NAME
             AND  x.END_DT = DATEADD(day, -1, $LOAD_DATE)
         )
  QUALIFY ROW_NUMBER() OVER (ORDER BY ID, NAME) = 1
),
src AS (
  SELECT 'SRC' AS SIDE, s.*
  FROM   QA_SRC_1 s
  JOIN   pick_inserts p ON p.ID = s.ID AND p.NAME = s.NAME
  WHERE  s.EXTRACT_DATE = $LOAD_DATE
    AND  COALESCE(s.IS_DELETED, FALSE) = FALSE
),
tgt_pre AS (
  SELECT 'TGT_PRE' AS SIDE, t.*
  FROM   QA_PRE_SCD2 t
  JOIN   pick_inserts p ON p.ID = t.ID AND p.NAME = t.NAME
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.ID, t.NAME
    ORDER BY GREATEST(COALESCE(t.START_DT, DATE '0001-01-01'),
                      COALESCE(t.END_DT,   DATE '0001-01-01')) DESC
  ) = 1
),
expected_post AS (
  SELECT 'EXPECTED_POST' AS SIDE,
         s.ID, s.NAME, s.EMAIL,
         $LOAD_DATE       AS START_DT,
         DATE '9999-12-31' AS END_DT,
         TRUE             AS CURRENT_FLG,
         FALSE            AS IS_DELETED,
         'SRC_1'          AS SRC_SYS_CD
  FROM   QA_SRC_1 s
  JOIN   pick_inserts p ON p.ID = s.ID AND p.NAME = s.NAME
  WHERE  s.EXTRACT_DATE = $LOAD_DATE
    AND  COALESCE(s.IS_DELETED, FALSE) = FALSE
),
tgt_post AS (
  SELECT 'TGT_POST' AS SIDE, t.*
  FROM   QA_POST_SCD2 t
  JOIN   pick_inserts p ON p.ID = t.ID AND p.NAME = t.NAME
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.ID, t.NAME
    ORDER BY GREATEST(COALESCE(t.START_DT, DATE '0001-01-01'),
                      COALESCE(t.END_DT,   DATE '0001-01-01')) DESC
  ) = 1
)
SELECT * FROM src
UNION ALL SELECT * FROM tgt_pre
UNION ALL SELECT * FROM expected_post
UNION ALL SELECT * FROM tgt_post
ORDER BY SIDE, START_DT NULLS LAST, END_DT NULLS LAST;

-- =====================================================================
-- 2) UPDATE VALIDATION — pick a PK that closed yesterday & opened today,
--    then show SRC_NEW, SRC_OLD (if present), TGT_PRE, EXPECTED_POST(2),
--    and TGT_POST(2)
-- =====================================================================
WITH
pick_pk AS (
  SELECT ID, NAME
  FROM   QA_POST_SCD2
  GROUP BY ID, NAME
  HAVING COUNT_IF(START_DT = $LOAD_DATE AND CURRENT_FLG = TRUE) = 1
     AND COUNT_IF(END_DT   = DATEADD(day, -1, $LOAD_DATE)) = 1
  QUALIFY ROW_NUMBER() OVER (ORDER BY ID, NAME) = 1
),
src_new AS (
  SELECT 'SRC_NEW' AS SIDE, s.*
  FROM   QA_SRC_1 s
  JOIN   pick_pk p ON p.ID = s.ID AND p.NAME = s.NAME
  WHERE  s.EXTRACT_DATE = $LOAD_DATE
    AND  COALESCE(s.IS_DELETED, FALSE) = FALSE
),
src_old AS (
  -- Only if your source also emits the closing (old) record today (optional)
  SELECT 'SRC_OLD' AS SIDE, s.*
  FROM   QA_SRC_1 s
  JOIN   pick_pk p ON p.ID = s.ID AND p.NAME = s.NAME
  WHERE  COALESCE(s.IS_DELETED, FALSE) = TRUE
    AND  s.EXTRACT_DATE = $LOAD_DATE
),
tgt_pre AS (
  SELECT 'TGT_PRE' AS SIDE, t.*
  FROM   QA_PRE_SCD2 t
  JOIN   pick_pk p ON p.ID = t.ID AND p.NAME = t.NAME
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.ID, t.NAME
    ORDER BY GREATEST(COALESCE(t.START_DT, DATE '0001-01-01'),
                      COALESCE(t.END_DT,   DATE '0001-01-01')) DESC
  ) = 1
),
expected_post AS (
  -- closed old
  SELECT 'EXPECTED_POST_OLD' AS SIDE,
         t.ID, t.NAME, t.EMAIL,
         t.START_DT,
         DATEADD(day,-1,$LOAD_DATE) AS END_DT,
         FALSE AS CURRENT_FLG,
         COALESCE(t.IS_DELETED, FALSE) AS IS_DELETED,
         'SRC_1' AS SRC_SYS_CD
  FROM   tgt_pre t
  UNION ALL
  -- new open
  SELECT 'EXPECTED_POST_NEW' AS SIDE,
         s.ID, s.NAME, s.EMAIL,
         $LOAD_DATE AS START_DT,
         DATE '9999-12-31' AS END_DT,
         TRUE  AS CURRENT_FLG,
         FALSE AS IS_DELETED,
         'SRC_1' AS SRC_SYS_CD
  FROM   src_new s
),
tgt_post AS (
  SELECT *
  FROM (
    SELECT 'TGT_POST' AS SIDE, t.*,
           ROW_NUMBER() OVER (
             PARTITION BY t.ID, t.NAME
             ORDER BY GREATEST(COALESCE(t.START_DT, DATE '0001-01-01'),
                               COALESCE(t.END_DT,   DATE '0001-01-01')) DESC
           ) rn
    FROM   QA_POST_SCD2 t
    JOIN   pick_pk p ON p.ID = t.ID AND p.NAME = t.NAME
  )
  WHERE rn <= 2
)
SELECT * FROM src_new
UNION ALL SELECT * FROM src_old
UNION ALL SELECT * FROM tgt_pre
UNION ALL SELECT * FROM expected_post
UNION ALL SELECT * FROM tgt_post
ORDER BY SIDE, START_DT NULLS LAST, END_DT NULLS LAST;

-- =====================================================================
-- 3) DELETE VALIDATION — pick a PK that is current & deleted today,
--    then show SRC_DELETE, TGT_PRE, EXPECTED_POST(2), and TGT_POST(2)
-- =====================================================================
WITH
pick_pk AS (
  SELECT ID, NAME
  FROM   QA_POST_SCD2
  WHERE  CURRENT_FLG = TRUE
    AND  COALESCE(IS_DELETED, 0) = 1
    AND  START_DT = $LOAD_DATE
  QUALIFY ROW_NUMBER() OVER (ORDER BY ID, NAME) = 1
),
src_del AS (
  SELECT 'SRC_DELETE' AS SIDE, s.*
  FROM   QA_SRC_1 s
  JOIN   pick_pk p ON p.ID = s.ID AND p.NAME = s.NAME
  WHERE  COALESCE(s.IS_DELETED, FALSE) = TRUE
    AND  s.EXTRACT_DATE = $LOAD_DATE
),
tgt_pre AS (
  SELECT 'TGT_PRE' AS SIDE, t.*
  FROM   QA_PRE_SCD2 t
  JOIN   pick_pk p ON p.ID = t.ID AND p.NAME = t.NAME
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.ID, t.NAME
    ORDER BY GREATEST(COALESCE(t.START_DT, DATE '0001-01-01'),
                      COALESCE(t.END_DT,   DATE '0001-01-01')) DESC
  ) = 1
),
expected_post AS (
  -- closed old
  SELECT 'EXPECTED_POST_OLD' AS SIDE,
         t.ID, t.NAME, t.EMAIL,
         t.START_DT,
         DATEADD(day,-1,$LOAD_DATE) AS END_DT,
         FALSE AS CURRENT_FLG,
         FALSE AS IS_DELETED,
         'SRC_1' AS SRC_SYS_CD
  FROM   tgt_pre t
  UNION ALL
  -- open delete row (if your policy wants no open row on delete, set CURRENT_FLG=FALSE and END_DT=$LOAD_DATE instead)
  SELECT 'EXPECTED_POST_DEL' AS SIDE,
         t.ID, t.NAME, t.EMAIL,
         $LOAD_DATE AS START_DT,
         DATE '9999-12-31' AS END_DT,
         TRUE  AS CURRENT_FLG,
         TRUE  AS IS_DELETED,
         'SRC_1' AS SRC_SYS_CD
  FROM   tgt_pre t
),
tgt_post AS (
  SELECT *
  FROM (
    SELECT 'TGT_POST' AS SIDE, t.*,
           ROW_NUMBER() OVER (
             PARTITION BY t.ID, t.NAME
             ORDER BY GREATEST(COALESCE(t.START_DT, DATE '0001-01-01'),
                               COALESCE(t.END_DT,   DATE '0001-01-01')) DESC
           ) rn
    FROM   QA_POST_SCD2 t
    JOIN   pick_pk p ON p.ID = t.ID AND p.NAME = t.NAME
  )
  WHERE rn <= 2
)
SELECT * FROM src_del
UNION ALL SELECT * FROM tgt_pre
UNION ALL SELECT * FROM expected_post
UNION ALL SELECT * FROM tgt_post
ORDER BY SIDE, START_DT NULLS LAST, END_DT NULLS LAST;
