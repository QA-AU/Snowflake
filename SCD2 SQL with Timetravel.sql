SCD2 SQL with Timetravel.py

-- Most recent MERGE on your dim table (adjust filters as needed)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_OBJECT(
  OBJECT_NAME => 'DIM_TABLE',
  OBJECT_SCHEMA => 'DIM_SCHEMA',
  RESULT_LIMIT => 50
))
WHERE QUERY_TEXT ILIKE '%MERGE%'
ORDER BY START_TIME DESC
LIMIT 5;








-- === INSERT: source new vs pre-merge dim (time travel) vs post-merge dim ===
WITH pick_pk AS (
  SELECT pk1, pk2
  FROM schema1.SCD_Insert_dt
  -- pick a specific PK or keep the first
  -- WHERE pk1 = :pk1_val AND pk2 = :pk2_val
  QUALIFY ROW_NUMBER() OVER (ORDER BY pk1, pk2) = 1
),
src_new AS (
  SELECT 'SOURCE_NEW' AS side, s.*
  FROM SRC_SCHEMA.TABLE1 s
  JOIN pick_pk p ON s.pk1 = p.pk1 AND s.pk2 = p.pk2
  WHERE s.extract_date = :load_date AND COALESCE(s.record_deleted_flag,0) = 0
),
dim_pre AS (
  -- state before MERGE (via time travel)
  SELECT 'DIM_PRE' AS side, d.*
  FROM DIM_SCHEMA.DIM_TABLE AT (TIMESTAMP => :pre_ts) d
  JOIN pick_pk p ON d.pk1 = p.pk1 AND d.pk2 = p.pk2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.pk1, d.pk2
    ORDER BY GREATEST(COALESCE(d.from_date, DATE '0001-01-01'),
                      COALESCE(d.to_date,   DATE '0001-01-01')) DESC
  ) = 1
),
dim_post AS (
  -- current state after MERGE
  SELECT 'DIM_POST' AS side, d.*
  FROM DIM_SCHEMA.DIM_TABLE d
  JOIN pick_pk p ON d.pk1 = p.pk1 AND d.pk2 = p.pk2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.pk1, d.pk2
    ORDER BY GREATEST(COALESCE(d.from_date, DATE '0001-01-01'),
                      COALESCE(d.to_date,   DATE '0001-01-01')) DESC
  ) = 1
)
SELECT * FROM src_new
UNION ALL
SELECT * FROM dim_pre
UNION ALL
SELECT * FROM dim_post
ORDER BY side;


-- === UPDATE: source new/old vs pre-merge dim vs two latest post-merge dim ===
WITH pick_pk AS (
  SELECT pk1, pk2
  FROM schema1.SCD_Update_dt
  -- WHERE pk1 = :pk1_val AND pk2 = :pk2_val
  QUALIFY ROW_NUMBER() OVER (ORDER BY pk1, pk2) = 1
),
src_new AS (
  SELECT 'SOURCE_NEW' AS side, s.*
  FROM SRC_SCHEMA.TABLE1 s
  JOIN pick_pk p ON s.pk1 = p.pk1 AND s.pk2 = p.pk2
  WHERE s.extract_date = :load_date AND COALESCE(s.record_deleted_flag,0) = 0
),
src_old AS (
  SELECT 'SOURCE_OLD' AS side, s.*
  FROM SRC_SCHEMA.TABLE1 s
  JOIN pick_pk p ON s.pk1 = p.pk1 AND s.pk2 = p.pk2
  WHERE s.logical_delete_date = :load_date
),
dim_pre AS (
  SELECT 'DIM_PRE' AS side, d.*
  FROM DIM_SCHEMA.DIM_TABLE AT (TIMESTAMP => :pre_ts) d
  JOIN pick_pk p ON d.pk1 = p.pk1 AND d.pk2 = p.pk2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.pk1, d.pk2
    ORDER BY GREATEST(COALESCE(d.from_date, DATE '0001-01-01'),
                      COALESCE(d.to_date,   DATE '0001-01-01')) DESC
  ) = 1
),
dim_post2 AS (
  SELECT *
  FROM (
    SELECT 'DIM_POST' AS side, d.*,
           ROW_NUMBER() OVER (
             PARTITION BY d.pk1, d.pk2
             ORDER BY GREATEST(COALESCE(d.from_date, DATE '0001-01-01'),
                               COALESCE(d.to_date,   DATE '0001-01-01')) DESC
           ) rn
    FROM DIM_SCHEMA.DIM_TABLE d
    JOIN pick_pk p ON d.pk1 = p.pk1 AND d.pk2 = p.pk2
  )
  WHERE rn <= 2
)
SELECT * FROM src_new
UNION ALL
SELECT * FROM src_old
UNION ALL
SELECT * FROM dim_pre
UNION ALL
SELECT * FROM dim_post2
ORDER BY side, from_date NULLS LAST, to_date NULLS LAST;


-- === DELETE: source delete vs pre-merge dim vs post-merge last ===
WITH pick_pk AS (
  SELECT pk1, pk2
  FROM schema1.SCD_Delete_dt
  -- WHERE pk1 = :pk1_val AND pk2 = :pk2_val
  QUALIFY ROW_NUMBER() OVER (ORDER BY pk1, pk2) = 1
),
src_del AS (
  SELECT 'SOURCE_DELETE' AS side, s.*
  FROM SRC_SCHEMA.TABLE1 s
  JOIN pick_pk p ON s.pk1 = p.pk1 AND s.pk2 = p.pk2
  WHERE COALESCE(s.record_deleted_flag,0) = 1
     OR s.logical_delete_date = :load_date
),
dim_pre AS (
  SELECT 'DIM_PRE' AS side, d.*
  FROM DIM_SCHEMA.DIM_TABLE AT (TIMESTAMP => :pre_ts) d
  JOIN pick_pk p ON d.pk1 = p.pk1 AND d.pk2 = p.pk2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.pk1, d.pk2
    ORDER BY GREATEST(COALESCE(d.from_date, DATE '0001-01-01'),
                      COALESCE(d.to_date,   DATE '0001-01-01')) DESC
  ) = 1
),
dim_post AS (
  SELECT 'DIM_POST' AS side, d.*
  FROM DIM_SCHEMA.DIM_TABLE d
  JOIN pick_pk p ON d.pk1 = p.pk1 AND d.pk2 = p.pk2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.pk1, d.pk2
    ORDER BY GREATEST(COALESCE(d.from_date, DATE '0001-01-01'),
                      COALESCE(d.to_date,   DATE '0001-01-01')) DESC
  ) = 1
)
SELECT * FROM src_del
UNION ALL
SELECT * FROM dim_pre
UNION ALL
SELECT * FROM dim_post
ORDER BY side, from_date NULLS LAST, to_date NULLS LAST;
