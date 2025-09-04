SCD2_Meta_Driven_v2.sql

--- Test Data given in the end --------

CREATE OR REPLACE TABLE qa_scd2_config (
    src_db         STRING,
    src_schema     STRING,
    src_table      STRING,
    tgt_db         STRING,
    tgt_schema     STRING,
    tgt_table      STRING,
    pk_cols        STRING,
    business_cols  STRING
);


INSERT INTO qa_scd2_config VALUES (
    'STAGING_DB', 
    'STG', 
    'CUSTOMER_STG',
    'DW_DB',
    'DIM',
    'CUSTOMER_DIM',
    'CUSTOMER_ID, REGION_ID',
    'NAME, ADDRESS'
);

-- Assign from config row
SET (src_db, src_schema, src_table,
     tgt_db, tgt_schema, tgt_table,
     pk_cols, business_cols) = (
    SELECT src_db, src_schema, src_table,
           tgt_db, tgt_schema, tgt_table,
           pk_cols, business_cols
    FROM qa_scd2_config
    LIMIT 1
);


SELECT $src_db   AS src_db,
       $src_schema AS src_schema,
       $src_table  AS src_table,
       $tgt_db   AS tgt_db,
       $tgt_schema AS tgt_schema,
       $tgt_table  AS tgt_table,
       $pk_cols    AS pk_cols,
       $business_cols AS business_cols;





-- ===========================================================
-- Step 0: Define parameters
-- ===========================================================
SET src_db      = 'STAGING_DB';
SET src_schema  = 'STG';
SET src_table   = 'CUSTOMER_STG';

SET tgt_db      = 'DW_DB';
SET tgt_schema  = 'DIM';
SET tgt_table   = 'CUSTOMER_DIM';

SET pk_cols     = 'CUSTOMER_ID, REGION_ID';   -- multi-PK supported
SET business_cols = 'NAME, ADDRESS';

-- Helper: build ON clause for multi-PKs
SET on_clause = (
    SELECT LISTAGG('tgt.' || TRIM(value) || ' = src.' || TRIM(value), ' AND ')
    FROM TABLE(SPLIT_TO_TABLE($pk_cols, ','))
);

SET pk_list_src = (
    SELECT LISTAGG('src.' || TRIM(value), ', ')
    FROM TABLE(SPLIT_TO_TABLE($pk_cols, ','))
);

SET pk_list_tgt = (
    SELECT LISTAGG('tgt.' || TRIM(value), ', ')
    FROM TABLE(SPLIT_TO_TABLE($pk_cols, ','))
);

-- ===========================================================
-- Step 1: Create staging with hash
-- ===========================================================
EXECUTE IMMEDIATE
'CREATE OR REPLACE TEMP TABLE qa_stg_hashed AS
 SELECT ' || $pk_cols || ',
        ' || $business_cols || ',
        HASH(' || $pk_cols || ', ' || $business_cols || ') AS hash_diff
 FROM ' || $src_db || '.' || $src_schema || '.' || $src_table;

-- ===========================================================
-- Step 2: Changed rows
-- ===========================================================
EXECUTE IMMEDIATE
'CREATE OR REPLACE TEMP TABLE qa_changed AS
 SELECT tgt.*
 FROM ' || $tgt_db || '.' || $tgt_schema || '.' || $tgt_table || ' tgt
 JOIN qa_stg_hashed src
   ON ' || $on_clause || '
 WHERE tgt.is_current = TRUE
   AND src.hash_diff <> tgt.hash_diff';

-- ===========================================================
-- Step 3: Expire changed rows
-- ===========================================================
EXECUTE IMMEDIATE
'CREATE OR REPLACE TEMP TABLE qa_expired AS
 SELECT ' || $pk_cols || ',
        ' || $business_cols || ',
        valid_from,
        CURRENT_DATE AS valid_to,
        FALSE AS is_current,
        hash_diff
 FROM qa_changed';

-- ===========================================================
-- Step 4: New versions of changed rows
-- ===========================================================
EXECUTE IMMEDIATE
'CREATE OR REPLACE TEMP TABLE qa_new_versions AS
 SELECT ' || $pk_list_src || ',
        ' || $business_cols || ',
        CURRENT_DATE AS valid_from,
        ''9999-12-31''::DATE AS valid_to,
        TRUE AS is_current,
        src.hash_diff
 FROM qa_stg_hashed src
 JOIN qa_changed c
   ON ' || $on_clause;

-- ===========================================================
-- Step 5: Brand new inserts
-- ===========================================================
EXECUTE IMMEDIATE
'CREATE OR REPLACE TEMP TABLE qa_new_inserts AS
 SELECT ' || $pk_list_src || ',
        ' || $business_cols || ',
        CURRENT_DATE AS valid_from,
        ''9999-12-31''::DATE AS valid_to,
        TRUE AS is_current,
        src.hash_diff
 FROM qa_stg_hashed src
 LEFT JOIN ' || $tgt_db || '.' || $tgt_schema || '.' || $tgt_table || ' tgt
   ON ' || $on_clause || '
 WHERE ' || $pk_list_tgt || ' IS NULL';

-- ===========================================================
-- Step 6: Final rebuild of dimension
-- ===========================================================
EXECUTE IMMEDIATE
'CREATE OR REPLACE TABLE ' || $tgt_db || '.' || $tgt_schema || '.' || $tgt_table || '_FINAL AS
 SELECT *
 FROM ' || $tgt_db || '.' || $tgt_schema || '.' || $tgt_table || '
 WHERE is_current = TRUE
   AND (' || $pk_cols || ') NOT IN (SELECT ' || $pk_cols || ' FROM qa_changed)
 UNION ALL
 SELECT * FROM qa_expired
 UNION ALL
 SELECT * FROM qa_new_versions
 UNION ALL
 SELECT * FROM qa_new_inserts';



-- Data setup

-- ===========================================================
-- Setup: Create Databases & Schemas
-- ===========================================================
CREATE OR REPLACE DATABASE STAGING_DB;
CREATE OR REPLACE SCHEMA STAGING_DB.STG;

CREATE OR REPLACE DATABASE DW_DB;
CREATE OR REPLACE SCHEMA DW_DB.DIM;

-- ===========================================================
-- Target Dimension Table (with SCD2 fields)
-- ===========================================================
CREATE OR REPLACE TABLE DW_DB.DIM.CUSTOMER_DIM (
    CUSTOMER_ID INT,
    REGION_ID   INT,
    NAME        STRING,
    ADDRESS     STRING,
    VALID_FROM  DATE,
    VALID_TO    DATE,
    IS_CURRENT  BOOLEAN,
    HASH_DIFF   STRING
);

-- Load initial dimension data
INSERT INTO DW_DB.DIM.CUSTOMER_DIM VALUES
    -- Alice active
    (1, 10, 'Alice', '123 Apple St', '2023-01-01', '9999-12-31', TRUE,  HASH(1,10,'Alice','123 Apple St')),
    -- Bob active
    (2, 20, 'Bob',   '456 Banana Rd', '2023-01-01', '9999-12-31', TRUE,  HASH(2,20,'Bob','456 Banana Rd')),
    -- Carol has history (old row expired, new row active)
    (3, 30, 'Carol', '789 Old Cherry Ln', '2023-01-01', '2023-12-31', FALSE, HASH(3,30,'Carol','789 Old Cherry Ln')),
    (3, 30, 'Carol', '789 Cherry Ln',     '2024-01-01', '9999-12-31', TRUE,  HASH(3,30,'Carol','789 Cherry Ln'));

-- ===========================================================
-- Staging Table (new batch data)
-- ===========================================================
CREATE OR REPLACE TABLE STAGING_DB.STG.CUSTOMER_STG (
    CUSTOMER_ID INT,
    REGION_ID   INT,
    NAME        STRING,
    ADDRESS     STRING
);

-- New staging load
INSERT INTO STAGING_DB.STG.CUSTOMER_STG VALUES
    (1, 10, 'Alice', '123 Apple Street'),  -- Alice changed (address updated)
    (2, 20, 'Bob',   '456 Banana Rd'),     -- Bob unchanged
    (4, 40, 'Dave',  '111 Date Dr');       -- New customer


SELECT * FROM qa_stg_hashed;
SELECT * FROM qa_changed;
SELECT * FROM qa_expired;
SELECT * FROM qa_new_versions;
SELECT * FROM qa_new_inserts;


SELECT * FROM DW_DB.DIM.CUSTOMER_DIM_FINAL ORDER BY CUSTOMER_ID, REGION_ID, VALID_FROM;
