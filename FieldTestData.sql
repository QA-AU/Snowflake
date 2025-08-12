-- Create Demo table for compare 

USE ROLE SNOWFLAKE_LEARNING_ROLE;

---> set the Warehouse
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> set the Database
USE DATABASE SNOWFLAKE_LEARNING_DB;

---> set the Schema
SET schema_name = CONCAT(current_user(), '_LOAD_SAMPLE_DATA_FROM_S3');
USE SCHEMA IDENTIFIER($schema_name);



CREATE OR REPLACE TEMPORARY PROCEDURE run_sql(sql_text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'tabulate')  -- tabulate makes nice tables
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from tabulate import tabulate
import traceback

def main(session: snowpark.Session, sql_text: str) -> str:
    try:
        df = session.sql(sql_text)
        rows = df.collect()
        if rows:
            # Get column names
            cols = df.schema.names
            # Convert Snowflake Rows to plain lists
            data = [list(r) for r in rows]
            # Pretty-print table (max 50 rows for display)
            return "\n" + tabulate(data[:50], headers=cols, tablefmt="grid")
        return "ok (no rows)"
    except Exception as e:
        return "ERROR: " + str(e) + "\n" + "".join(traceback.format_exc(limit=2))
$$;


CALL run_sql('CREATE SCHEMA IF NOT EXISTS SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3');

CALL run_sql('CREATE SCHEMA IF NOT EXISTS SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3');


-- Source table (has system columns to ignore)
CALL run_sql($$
CREATE OR REPLACE TABLE SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.CUSTOMERS(
  ID INT,
  NAME STRING,
  LOAD_TS TIMESTAMP_NTZ,
  _INGEST_ID STRING
)
$$);

-- Target table (different system cols to ignore)
CALL run_sql($$
CREATE OR REPLACE TABLE SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.DIM_CUSTOMER(
  ID INT,
  NAME STRING,
  _ETL_TS TIMESTAMP_NTZ,
  HASHDIFF STRING
)
$$);

-- Insert matching business data (ID, NAME) on both sides
CALL run_sql($$
INSERT INTO SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.CUSTOMERS(ID, NAME, LOAD_TS, _INGEST_ID)
SELECT 1, 'Alice', CURRENT_TIMESTAMP, 'ing-1' UNION ALL
SELECT 2, 'Bob',   CURRENT_TIMESTAMP, 'ing-2' UNION ALL
SELECT 3, 'Cara',  CURRENT_TIMESTAMP, 'ing-3'
$$);

CALL run_sql($$
INSERT INTO SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.DIM_CUSTOMER(ID, NAME, _ETL_TS, HASHDIFF)
SELECT 1, 'Alice', CURRENT_TIMESTAMP, 'h1' UNION ALL
SELECT 2, 'Bob',   CURRENT_TIMESTAMP, 'h2' UNION ALL
SELECT 3, 'Cara',  CURRENT_TIMESTAMP, 'h3'
$$);





CALL run_sql($$
CREATE OR REPLACE TABLE SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.ORDERS(
  ORDER_ID INT,
  AMOUNT NUMBER(10,2),
  LOAD_TS TIMESTAMP_NTZ,
  _INGEST_ID STRING
)
$$);

CALL run_sql($$
CREATE OR REPLACE TABLE SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.FACT_ORDER(
  ORDER_ID INT,
  AMOUNT NUMBER(10,2),
  _ETL_TS TIMESTAMP_NTZ
)
$$);

-- Intentionally create differences:
--  - ORDER_ID 1002 has different AMOUNT between S and T
--  - ORDER_ID 1003 exists only in source
--  - ORDER_ID 2000 exists only in target
CALL run_sql($$
INSERT INTO SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.ORDERS(ORDER_ID, AMOUNT, LOAD_TS, _INGEST_ID)
SELECT 1001, 49.90, CURRENT_TIMESTAMP, 'ing-a' UNION ALL
SELECT 1002, 10.00, CURRENT_TIMESTAMP, 'ing-b' UNION ALL
SELECT 1003,  5.00, CURRENT_TIMESTAMP, 'ing-c'
$$);

CALL run_sql($$
INSERT INTO SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.FACT_ORDER(ORDER_ID, AMOUNT, _ETL_TS)
SELECT 1001, 49.90, CURRENT_TIMESTAMP UNION ALL
SELECT 1002, 11.00, CURRENT_TIMESTAMP UNION ALL
SELECT 2000, 99.99, CURRENT_TIMESTAMP
$$);




CALL run_sql($$
CREATE OR REPLACE TABLE META_TABLE_MAP (
  RULE_ID            NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  S_SCHEMA           STRING         NOT NULL,
  S_TABLE            STRING         NOT NULL,
  S_IGNORE_COLUMNS   VARIANT,       -- JSON array (e.g., ["LOAD_TS","_INGEST_ID"])
  T_SCHEMA           STRING         NOT NULL,
  T_TABLE            STRING         NOT NULL,
  T_IGNORE_COLUMNS   VARIANT,       -- JSON array (e.g., ["_ETL_TS","HASHDIFF"])
  IS_ACTIVE          BOOLEAN        DEFAULT TRUE,
  NOTE               STRING,
  CREATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
$$);




-- Matching pair mapping
CALL run_sql($$
INSERT INTO META_TABLE_MAP
  (S_SCHEMA, S_TABLE, S_IGNORE_COLUMNS, T_SCHEMA, T_TABLE, T_IGNORE_COLUMNS, NOTE)
SELECT
  'SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3', 'CUSTOMERS', PARSE_JSON('["LOAD_TS","_INGEST_ID"]'),
  'SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3',  'DIM_CUSTOMER', PARSE_JSON('["_ETL_TS","HASHDIFF"]'),
  'Demo: customers should PASS (business cols match)'
$$);

-- Mismatching pair mapping
CALL run_sql($$
INSERT INTO META_TABLE_MAP
  (S_SCHEMA, S_TABLE, S_IGNORE_COLUMNS, T_SCHEMA, T_TABLE, T_IGNORE_COLUMNS, NOTE)
SELECT
  'SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3', 'ORDERS', PARSE_JSON('["LOAD_TS","_INGEST_ID"]'),
  'SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3',  'FACT_ORDER', PARSE_JSON('["_ETL_TS"]'),
  'Demo: orders should FAIL (diff rows/values)'
$$);


CALL run_sql('SELECT RULE_ID, S_SCHEMA, S_TABLE, T_SCHEMA, T_TABLE, S_IGNORE_COLUMNS, T_IGNORE_COLUMNS FROM META_TABLE_MAP ORDER BY RULE_ID');


select * from META_TABLE_MAP 


