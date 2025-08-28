scd2_test_with_data_v3.py


--created on 28-Aug-25

--crete sample data
OK ? 


CREATE OR REPLACE TABLE SRC1_CUSTOMER (
    ID            INT,
    NAME          STRING,
    EMAIL         STRING,
    EXTRACT_DATE  DATE,
    IS_DELETED    BOOLEAN
);

-- Sample feed for 2025-01-07
INSERT INTO SRC1_CUSTOMER VALUES
(1, 'Alice', 'alice@abc.com', '2025-01-07', FALSE),   -- unchanged
(2, 'Bob',   'bob.new@abc.com', '2025-01-07', FALSE), -- updated email
(3, 'Charlie', 'charlie@abc.com', '2025-01-07', TRUE),-- logical delete
(4, 'Diana', 'diana@abc.com', '2025-01-07', FALSE);   -- brand new



CREATE OR REPLACE TABLE TGT_PRE_SCD2 (
    ID            INT,
    NAME          STRING,
    EMAIL         STRING,
    START_DT      DATE,
    END_DT        DATE,
    CURRENT_FLG   BOOLEAN,
    IS_DELETED    BOOLEAN,
    SRC_SYS_CD    STRING
);

INSERT INTO TGT_PRE_SCD2 VALUES
(1, 'Alice',   'alice@abc.com', '2025-01-01', '9999-12-31', TRUE,  FALSE, 'SRC_1'),
(2, 'Bob',     'bob@abc.com',   '2025-01-01', '9999-12-31', TRUE,  FALSE, 'SRC_1'),
(3, 'Charlie', 'charlie@abc.com','2025-01-01','9999-12-31', TRUE,  FALSE, 'SRC_1');


CREATE OR REPLACE TABLE TGT_POST_SCD2 (
    ID            INT,
    NAME          STRING,
    EMAIL         STRING,
    START_DT      DATE,
    END_DT        DATE,
    CURRENT_FLG   BOOLEAN,
    IS_DELETED    BOOLEAN,
    SRC_SYS_CD    STRING
);

INSERT INTO TGT_POST_SCD2 VALUES
-- Alice unchanged
(1, 'Alice', 'alice@abc.com',  '2025-01-01', '9999-12-31', TRUE,  FALSE, 'SRC_1'),

-- Bob updated
(2, 'Bob',   'bob@abc.com',    '2025-01-01', '2025-01-06', FALSE, FALSE, 'SRC_1'),
(2, 'Bob',   'bob.new@abc.com','2025-01-07', '9999-12-31', TRUE,  FALSE, 'SRC_1'),

-- Charlie deleted
(3, 'Charlie','charlie@abc.com','2025-01-01', '2025-01-06', FALSE, FALSE, 'SRC_1'),
(3, 'Charlie','charlie@abc.com','2025-01-07', '9999-12-31', TRUE,  TRUE,  'SRC_1'),

-- Diana new insert
(4, 'Diana', 'diana@abc.com', '2025-01-07', '9999-12-31', TRUE,  FALSE, 'SRC_1');


---TEST----

-- === PARAMETERS ===
SET LOAD_DATE         = '2025-01-07'::DATE;         -- processing date
SET SRC_CODE          = 'SRC_1';                    -- 'SRC_1' or 'SRC_2'
SET PRE_TS            = '2025-01-07 01:59:59'::TIMESTAMP;  -- time-travel timestamp just BEFORE MERGE

-- Open-end handling
SET USE_NULL_OPEN_END = FALSE;                      -- TRUE => open end = NULL
SET OPEN_END_DATE     = '9999-12-31'::DATE;

-- Helpers
SET OPEN_END_EVAL     = IFF($USE_NULL_OPEN_END, NULL, $OPEN_END_DATE);


---INSERT TEst- 

-- Pick a sample PK from ACTUAL post SCD2 where the row started today and came from SRC_1 (non-deleted)
WITH pick_pk AS (
  SELECT pk1, pk2
  FROM   TGT_SCHEMA.TGT_Post_SCD2
  WHERE  start_dt = $LOAD_DATE
    AND  current_flg = TRUE
    AND  COALESCE(is_deleted,0) = 0
    AND  src_sys_cd = 'SRC_1'
  QUALIFY ROW_NUMBER() OVER (ORDER BY pk1, pk2) = 1
),

-- Source (today’s new row)
src AS (
  SELECT 'SRC' AS side, s.*
  FROM   SRC1_SCHEMA.TABLE1 s
  JOIN   pick_pk p ON p.pk1 = s.pk1 AND p.pk2 = s.pk2
  WHERE  s.extract_date = $LOAD_DATE
    AND  COALESCE(s.record_deleted_flag,0) = 0
),

-- Target PRE (time travel) – latest row before merge (if existed)
tgt_pre AS (
  SELECT 'TGT_PRE' AS side, t.*
  FROM   TGT_SCHEMA.TGT_Post_SCD2 AT (TIMESTAMP => $PRE_TS) t
  JOIN   pick_pk p ON p.pk1 = t.pk1 AND p.pk2 = t.pk2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.pk1,t.pk2
           ORDER BY GREATEST(COALESCE(t.start_dt,DATE '0001-01-01'),
                             COALESCE(t.end_dt,  DATE '0001-01-01')) DESC) = 1
),

-- EXPECTED post row (for insert)
expected_post AS (
  SELECT 'EXPECTED_POST' AS side,
         s.pk1, s.pk2,
         s.attr1, s.attr2, /* ... all tracked attributes ... */
         $LOAD_DATE  AS start_dt,
         $OPEN_END_EVAL AS end_dt,
         TRUE        AS current_flg,
         0           AS is_deleted,
         'SRC_1'     AS src_sys_cd
  FROM   SRC1_SCHEMA.TABLE1 s
  JOIN   pick_pk p ON p.pk1 = s.pk1 AND p.pk2 = s.pk2
  WHERE  s.extract_date = $LOAD_DATE AND COALESCE(s.record_deleted_flag,0) = 0
),

-- ACTUAL post row (current)
tgt_post AS (
  SELECT 'TGT_POST' AS side, t.*
  FROM   TGT_SCHEMA.TGT_Post_SCD2 t
  JOIN   pick_pk p ON p.pk1 = t.pk1 AND p.pk2 = t.pk2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.pk1,t.pk2
           ORDER BY GREATEST(COALESCE(t.start_dt,DATE '0001-01-01'),
                             COALESCE(t.end_dt,  DATE '0001-01-01')) DESC) = 1
)
SELECT * FROM src
UNION ALL SELECT * FROM tgt_pre
UNION ALL SELECT * FROM expected_post
UNION ALL SELECT * FROM tgt_post
ORDER BY side, start_dt NULLS LAST, end_dt NULLS LAST;



--UPDATE TEST---

WITH pick_pk AS (
  /* Pick a PK that has a new row starting today AND a closed row ending yesterday */
  SELECT pk1, pk2
  FROM   TGT_SCHEMA.TGT_Post_SCD2
  WHERE  src_sys_cd = 'SRC_1'
  GROUP BY pk1, pk2
  HAVING COUNT_IF(start_dt = $LOAD_DATE AND current_flg = TRUE) = 1
     AND COUNT_IF(end_dt = DATEADD(day,-1,$LOAD_DATE)) = 1
  QUALIFY ROW_NUMBER() OVER (ORDER BY pk1, pk2) = 1
),

src_new AS (  -- the arriving version
  SELECT 'SRC_NEW' AS side, s.*
  FROM   SRC1_SCHEMA.TABLE1 s
  JOIN   pick_pk p ON p.pk1 = s.pk1 AND p.pk2 = s.pk2
  WHERE  s.extract_date = $LOAD_DATE AND COALESCE(s.record_deleted_flag,0) = 0
),

src_old AS (  -- the prior version closed today in the feed (optional if present)
  SELECT 'SRC_OLD' AS side, s.*
  FROM   SRC1_SCHEMA.TABLE1 s
  JOIN   pick_pk p ON p.pk1 = s.pk1 AND p.pk2 = s.pk2
  WHERE  s.logical_delete_date = $LOAD_DATE    -- if your feed sends the closing version
),

tgt_pre AS (
  SELECT 'TGT_PRE' AS side, t.*
  FROM   TGT_SCHEMA.TGT_Post_SCD2 AT (TIMESTAMP => $PRE_TS) t
  JOIN   pick_pk p ON p.pk1 = t.pk1 AND p.pk2 = t.pk2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.pk1,t.pk2
           ORDER BY GREATEST(COALESCE(t.start_dt,DATE '0001-01-01'),
                             COALESCE(t.end_dt,  DATE '0001-01-01')) DESC) = 1
),

expected_post AS ( -- two rows: closed old + new open
  SELECT 'EXPECTED_POST_OLD' AS side,
         t.pk1, t.pk2, t.attr1, t.attr2, /* ... */ 
         t.start_dt,
         DATEADD(day,-1,$LOAD_DATE) AS end_dt,
         FALSE AS current_flg,
         COALESCE(t.is_deleted,0) AS is_deleted,
         'SRC_1' AS src_sys_cd
  FROM   tgt_pre t
  UNION ALL
  SELECT 'EXPECTED_POST_NEW' AS side,
         s.pk1, s.pk2, s.attr1, s.attr2, /* ... */
         $LOAD_DATE AS start_dt,
         $OPEN_END_EVAL AS end_dt,
         TRUE AS current_flg,
         0    AS is_deleted,
         'SRC_1' AS src_sys_cd
  FROM   src_new s
),

tgt_post AS (   -- fetch two latest rows now
  SELECT *
  FROM (
    SELECT 'TGT_POST' AS side, t.*,
           ROW_NUMBER() OVER (PARTITION BY t.pk1,t.pk2
             ORDER BY GREATEST(COALESCE(t.start_dt,DATE '0001-01-01'),
                               COALESCE(t.end_dt,  DATE '0001-01-01')) DESC) rn
    FROM   TGT_SCHEMA.TGT_Post_SCD2 t
    JOIN   pick_pk p ON p.pk1 = t.pk1 AND p.pk2 = t.pk2
  )
  WHERE rn <= 2
)
SELECT * FROM src_new
UNION ALL SELECT * FROM src_old
UNION ALL SELECT * FROM tgt_pre
UNION ALL SELECT * FROM expected_post
UNION ALL SELECT * FROM tgt_post
ORDER BY side, start_dt NULLS LAST, end_dt NULLS LAST;


---DELETE TEST---


WITH pick_pk AS (
  SELECT pk1, pk2
  FROM   TGT_SCHEMA.TGT_Post_SCD2
  WHERE  src_sys_cd = 'SRC_1'
    AND  current_flg = TRUE
    AND  COALESCE(is_deleted,0) = 1
    AND  start_dt = $LOAD_DATE
  QUALIFY ROW_NUMBER() OVER (ORDER BY pk1, pk2) = 1
),

src_del AS (
  SELECT 'SRC_DELETE' AS side, s.*
  FROM   SRC1_SCHEMA.TABLE1 s
  JOIN   pick_pk p ON p.pk1 = s.pk1 AND p.pk2 = s.pk2
  WHERE  COALESCE(s.record_deleted_flag,0) = 1
     OR  s.logical_delete_date = $LOAD_DATE
),

tgt_pre AS (
  SELECT 'TGT_PRE' AS side, t.*
  FROM   TGT_SCHEMA.TGT_Post_SCD2 AT (TIMESTAMP => $PRE_TS) t
  JOIN   pick_pk p ON p.pk1 = t.pk1 AND p.pk2 = t.pk2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.pk1,t.pk2
           ORDER BY GREATEST(COALESCE(t.start_dt,DATE '0001-01-01'),
                             COALESCE(t.end_dt,  DATE '0001-01-01')) DESC) = 1
),

expected_post AS (
  SELECT 'EXPECTED_POST_OLD' AS side,
         t.pk1, t.pk2, t.attr1, t.attr2, /* ... */
         t.start_dt,
         DATEADD(day,-1,$LOAD_DATE) AS end_dt,
         FALSE AS current_flg,
         0     AS is_deleted,
         'SRC_1' AS src_sys_cd
  FROM   tgt_pre t
  UNION ALL
  SELECT 'EXPECTED_POST_DEL' AS side,
         t.pk1, t.pk2, t.attr1, t.attr2, /* carry last attrs or sparse delete row */
         $LOAD_DATE AS start_dt,
         $OPEN_END_EVAL AS end_dt,
         TRUE  AS current_flg,
         1     AS is_deleted,
         'SRC_1' AS src_sys_cd
  FROM   tgt_pre t
),

tgt_post AS (
  SELECT *
  FROM (
    SELECT 'TGT_POST' AS side, t.*,
           ROW_NUMBER() OVER (PARTITION BY t.pk1,t.pk2
             ORDER BY GREATEST(COALESCE(t.start_dt,DATE '0001-01-01'),
                               COALESCE(t.end_dt,  DATE '0001-01-01')) DESC) rn
    FROM   TGT_SCHEMA.TGT_Post_SCD2 t
    JOIN   pick_pk p ON p.pk1 = t.pk1 AND p.pk2 = t.pk2
  )
  WHERE rn <= 2
)
SELECT * FROM src_del
UNION ALL SELECT * FROM tgt_pre
UNION ALL SELECT * FROM expected_post
UNION ALL SELECT * FROM tgt_post
ORDER BY side, start_dt NULLS LAST, end_dt NULLS LAST;


