snowflake meta validation.py

-- =====================================================
-- Step 1: Create sample source tables
-- =====================================================
CREATE OR REPLACE TABLE SRC.CUSTOMERS (
    CUSTOMER_ID   INT,
    EMAIL         STRING,
    PHONE         STRING,
    DOB           DATE,
    COUNTRY       STRING
);

CREATE OR REPLACE TABLE SRC.ORDERS (
    ORDER_ID      INT,
    CUSTOMER_ID   INT,
    PRODUCT       STRING,
    ADDRESS       STRING,
    ORDER_DATE    DATE
);

-- Insert sample data
INSERT INTO SRC.CUSTOMERS VALUES
    (1, 'alice@test.com', '0412345', '1990-01-01', 'AU'),
    (2, 'bob@example.org', '0499999', '1985-07-12', 'AU'),
    (3, 'charlie@mail.com', '0400000', '2000-03-15', 'NZ');

INSERT INTO SRC.ORDERS VALUES
    (101, 1, 'Laptop',  '123 Main St', '2024-01-01'),
    (102, 1, 'Phone',   '123 Main St', '2024-01-05'),
    (103, 2, 'Monitor', '45 Queen Rd', '2024-02-10');

-- =====================================================
-- Step 2: Create meta table
-- =====================================================
CREATE OR REPLACE TABLE META_COLUMNS (
    slayer     STRING,
    srctable   STRING,
    src_col    STRING,
    datatype   STRING,
    format     STRING,
    col_order  INT,
    pk_flag    STRING,
    pii_flag   STRING
);

-- =====================================================
-- Step 3: Insert metadata
-- =====================================================

-- Metadata for CUSTOMERS
INSERT INTO META_COLUMNS VALUES
('SRC', 'CUSTOMERS', 'CUSTOMER_ID', 'INT', NULL, 1, 'Y', 'N'),
('SRC', 'CUSTOMERS', 'EMAIL',       'STRING', NULL, 2, 'N', 'Y'),
('SRC', 'CUSTOMERS', 'PHONE',       'STRING', NULL, 3, 'N', 'Y'),
('SRC', 'CUSTOMERS', 'DOB',         'DATE',   'YYYY-MM-DD', 4, 'N', 'N'),
('SRC', 'CUSTOMERS', 'COUNTRY',     'STRING', NULL, 5, 'N', 'N');

-- Metadata for ORDERS
INSERT INTO META_COLUMNS VALUES
('SRC', 'ORDERS', 'ORDER_ID',    'INT',    NULL, 1, 'Y', 'N'),
('SRC', 'ORDERS', 'CUSTOMER_ID', 'INT',    NULL, 2, 'N', 'N'),
('SRC', 'ORDERS', 'PRODUCT',     'STRING', NULL, 3, 'N', 'N'),
('SRC', 'ORDERS', 'ADDRESS',     'STRING', NULL, 4, 'N', 'Y'),
('SRC', 'ORDERS', 'ORDER_DATE',  'DATE',   'YYYY-MM-DD', 5, 'N', 'N');

-- =====================================================
-- Step 4: Validation SQLs (Steps 1–8)
-- =====================================================

-- 4.1: Validate PK defined
SELECT slayer, srctable,
       CASE WHEN SUM(CASE WHEN UPPER(pk_flag)='Y' THEN 1 ELSE 0 END) > 0 
            THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM META_COLUMNS
GROUP BY slayer, srctable;

-- 4.2: Validate column existence in schema
SELECT m.slayer, m.srctable, m.src_col,
       CASE WHEN c.COLUMN_NAME IS NOT NULL THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM META_COLUMNS m
LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
       ON c.TABLE_SCHEMA = m.slayer
      AND c.TABLE_NAME   = m.srctable
      AND c.COLUMN_NAME  = m.src_col;

-- 4.3: Validate duplicate column definitions
SELECT slayer, srctable, src_col,
       CASE WHEN COUNT(*)=1 THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM META_COLUMNS
GROUP BY slayer, srctable, src_col;

-- 4.4: Validate datatype consistency
SELECT m.slayer, m.srctable, m.src_col,
       m.datatype AS expected_type, c.DATA_TYPE AS actual_type,
       CASE WHEN UPPER(m.datatype)=UPPER(c.DATA_TYPE) THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM META_COLUMNS m
JOIN INFORMATION_SCHEMA.COLUMNS c
  ON c.TABLE_SCHEMA=m.slayer
 AND c.TABLE_NAME=m.srctable
 AND c.COLUMN_NAME=m.src_col;

-- 4.5: Validate column order
SELECT m.slayer, m.srctable, m.src_col,
       m.col_order AS expected_order, c.ORDINAL_POSITION AS actual_order,
       CASE WHEN m.col_order=c.ORDINAL_POSITION THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM META_COLUMNS m
JOIN INFORMATION_SCHEMA.COLUMNS c
  ON c.TABLE_SCHEMA=m.slayer
 AND c.TABLE_NAME=m.srctable
 AND c.COLUMN_NAME=m.src_col;

-- 4.6: Validate null PKs (manual per table)
SELECT COUNT(*) AS null_pk_count,
       CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM SRC.CUSTOMERS
WHERE CUSTOMER_ID IS NULL;

SELECT COUNT(*) AS null_pk_count,
       CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM SRC.ORDERS
WHERE ORDER_ID IS NULL;

-- 4.7: Validate duplicate PK values (manual per table)
SELECT COUNT(*) AS dup_count,
       CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM (
    SELECT CUSTOMER_ID, COUNT(*) c
    FROM SRC.CUSTOMERS
    GROUP BY CUSTOMER_ID
    HAVING COUNT(*) > 1
);

SELECT COUNT(*) AS dup_count,
       CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM (
    SELECT ORDER_ID, COUNT(*) c
    FROM SRC.ORDERS
    GROUP BY ORDER_ID
    HAVING COUNT(*) > 1
);

-- 4.8: Validate column format (against metadata)
SELECT m.slayer, m.srctable, m.src_col,
       m.format AS expected_format,
       CASE 
         WHEN c.DATA_TYPE IN ('DATE','TIMESTAMP') THEN 'YYYY-MM-DD'
         WHEN c.DATA_TYPE LIKE 'NUMBER%' THEN 
              'NUMERIC(' || c.NUMERIC_PRECISION || ',' || COALESCE(c.NUMERIC_SCALE,0) || ')'
         WHEN c.DATA_TYPE LIKE 'VARCHAR%' THEN 
              'VARCHAR(' || c.CHARACTER_MAXIMUM_LENGTH || ')'
         ELSE c.DATA_TYPE
       END AS actual_format,
       CASE WHEN m.format IS NULL 
                 OR UPPER(m.format) = UPPER(
                       CASE 
                         WHEN c.DATA_TYPE IN ('DATE','TIMESTAMP') THEN 'YYYY-MM-DD'
                         WHEN c.DATA_TYPE LIKE 'NUMBER%' THEN 
                              'NUMERIC(' || c.NUMERIC_PRECISION || ',' || COALESCE(c.NUMERIC_SCALE,0) || ')'
                         WHEN c.DATA_TYPE LIKE 'VARCHAR%' THEN 
                              'VARCHAR(' || c.CHARACTER_MAXIMUM_LENGTH || ')'
                         ELSE c.DATA_TYPE
                       END
                 ) 
            THEN 'PASS' ELSE 'FAIL' END AS validation_result
FROM META_COLUMNS m
JOIN INFORMATION_SCHEMA.COLUMNS c
  ON c.TABLE_SCHEMA=m.slayer
 AND c.TABLE_NAME=m.srctable
 AND c.COLUMN_NAME=m.src_col;

-- =====================================================
-- Step 5: Create PII Testing Procedure
-- =====================================================
CREATE OR REPLACE PROCEDURE VALIDATE_PII_SAMPLES()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session

def run(session: Session) -> str:

    meta_df = session.table("META_COLUMNS").filter("pii_flag = 'Y'")
    meta_pd = meta_df.to_pandas()

    if meta_pd.empty:
        return "No PII columns found in META_COLUMNS"

    for (slayer, srctable), group in meta_pd.groupby(["SLAYER","SRCTABLE"]):
        pii_cols = group["SRC_COL"].tolist()
        col_list = ", ".join(pii_cols)

        sql_stmt = f"SELECT DISTINCT {col_list} FROM {slayer}.{srctable} LIMIT 10"

        try:
            print(f"\n=== PII Samples from {slayer}.{srctable} ===")
            session.sql(sql_stmt).show()
        except Exception as e:
            print(f"\nERROR in {slayer}.{srctable}: {str(e)}")

    return "PII samples displayed per table"
$$;

-- =====================================================
-- Step 6: Run the PII Validation Procedure
-- =====================================================
CALL VALIDATE_PII_SAMPLES();


