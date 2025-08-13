-- comparetable_transposeresult.sql   - tested on 13/08/25

--  - Block 1-------------------------
-- 1) Save the two sample rows into a TEMP TABLE (note the WITH is attached to CREATE ... AS SELECT)
CREATE OR REPLACE TEMP TABLE temp_sample AS
WITH src AS (
    SELECT *
    FROM SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.customers
),
tgt AS (
    SELECT *
    FROM SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.dim_customer
),
only_in_src AS (
    SELECT * FROM src
    MINUS
    SELECT * FROM tgt
),
only_in_tgt AS (
    SELECT * FROM tgt
    MINUS
    SELECT * FROM src
),
diff_pk AS (
    SELECT id FROM only_in_src
    UNION
    SELECT id FROM only_in_tgt
    LIMIT 1
),
sample_union AS (
    SELECT 'source' AS side, s.*
    FROM src s
    JOIN diff_pk d ON s.id = d.id
    UNION ALL
    SELECT 'target' AS side, t.*
    FROM tgt t
    JOIN diff_pk d ON t.id = d.id
    --- USER to modify joins above as required. 
)
SELECT * FROM sample_union;



--  - Block 2-------------------------

-- 2) Transpose into one-row-per-column with a Match/Mismatch flag
CREATE OR REPLACE TEMP TABLE temp_sample_transpose AS
WITH
src_obj AS (
  SELECT OBJECT_DELETE(OBJECT_CONSTRUCT(*), 'SIDE') AS o
  FROM temp_sample
  WHERE side = 'source'
  LIMIT 1
),
tgt_obj AS (
  SELECT OBJECT_DELETE(OBJECT_CONSTRUCT(*), 'SIDE') AS o
  FROM temp_sample
  WHERE side = 'target'
  LIMIT 1
),
src_flat AS (
  SELECT f.key   AS column_name,
         f.value AS src_value
  FROM src_obj, LATERAL FLATTEN(input => o) f
),
tgt_flat AS (
  SELECT f.key   AS column_name,
         f.value AS tgt_value
  FROM tgt_obj, LATERAL FLATTEN(input => o) f
),
joined AS (
  SELECT COALESCE(s.column_name, t.column_name) AS column_name,
         s.src_value,
         t.tgt_value
  FROM src_flat s
  FULL OUTER JOIN tgt_flat t
    ON s.column_name = t.column_name
)
SELECT
  column_name,
  TO_VARCHAR(src_value) AS source_value,
  TO_VARCHAR(tgt_value) AS target_value,
  CASE
    WHEN src_value IS NOT DISTINCT FROM tgt_value THEN 'Match'
    ELSE 'Mismatch'
  END AS match_status
FROM joined;

--  - Block 3-------------------------
-- 3) Display the result (mismatches first)
SELECT *
FROM temp_sample_transpose
ORDER BY (match_status = 'Mismatch') DESC, column_name;
