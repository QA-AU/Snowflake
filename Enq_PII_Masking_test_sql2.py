Enq_PII_Masking_test_sql2.py

CREATE OR REPLACE PROCEDURE TEST_PII_COLUMNS(SCHEMA_NAME STRING, TABLE_NAME STRING)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col

def run(session: Session, SCHEMA_NAME: str, TABLE_NAME: str):

    # Step 1: Get PII-tagged columns
    query = f"""
        SELECT COLUMN_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE TAG_NAME = 'PII'
          AND OBJECT_DOMAIN = 'COLUMN'
          AND OBJECT_SCHEMA = UPPER('{SCHEMA_NAME}')
          AND OBJECT_NAME = UPPER('{TABLE_NAME}')
    """
    pii_cols_df = session.sql(query).collect()

    if not pii_cols_df:
        return session.sql(f"SELECT 'No PII columns found for {SCHEMA_NAME}.{TABLE_NAME}' AS message")

    pii_cols = [row.COLUMN_NAME for row in pii_cols_df]

    # Step 2: Build dynamic SELECT on PII columns
    sql_stmt = f"""
        SELECT {', '.join(pii_cols)}
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        LIMIT 10
    """

    # Step 3: Return result set
    return session.sql(sql_stmt)
$$;

-- Example: test masking on table X in schema PUBLIC
CALL TEST_PII_COLUMNS('PUBLIC', 'FACT_ORDER');



-------------Scean all  tables in a schema

CREATE OR REPLACE PROCEDURE TEST_PII_SCHEMA(SCHEMA_NAME STRING)
RETURNS TABLE (TABLE_NAME STRING, COLUMN_VALUES VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.session import Session
from snowflake.snowpark import Row

def run(session: Session, SCHEMA_NAME: str):

    # Step 1: Find all tables + columns with PII tag in this schema
    query = f"""
        SELECT OBJECT_NAME AS TABLE_NAME, COLUMN_NAME
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE TAG_NAME = 'PII'
          AND OBJECT_DOMAIN = 'COLUMN'
          AND OBJECT_SCHEMA = UPPER('{SCHEMA_NAME}')
        ORDER BY TABLE_NAME, COLUMN_NAME
    """
    pii_refs = session.sql(query).collect()

    if not pii_refs:
        return session.create_dataframe(
            [Row(TABLE_NAME="*", COLUMN_VALUES="No PII columns found in schema " + SCHEMA_NAME)]
        )

    results = []

    # Step 2: Group by table and query each
    tables = {}
    for row in pii_refs:
        tables.setdefault(row.TABLE_NAME, []).append(row.COLUMN_NAME)

    for table, cols in tables.items():
        col_list = ", ".join(cols)
        sql_stmt = f"SELECT {col_list} FROM {SCHEMA_NAME}.{table} LIMIT 10"
        try:
            df = session.sql(sql_stmt).collect()
            for record in df:
                results.append(Row(TABLE_NAME=table, COLUMN_VALUES=record.as_dict()))
        except Exception as e:
            results.append(Row(TABLE_NAME=table, COLUMN_VALUES=f"Error querying table: {str(e)}"))

    return session.create_dataframe(results)
$$;


-- Example: test all PII columns in schema PUBLIC
CALL TEST_PII_SCHEMA('PUBLIC');

---------Pick Imput from a table


CREATE OR REPLACE PROCEDURE TEST_PII_FROM_META(META_SCHEMA STRING, META_TABLE STRING, TARGET_SCHEMA STRING)
RETURNS TABLE (TABLE_NAME STRING, COLUMN_VALUES VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.session import Session
from snowflake.snowpark import Row

def run(session: Session, META_SCHEMA: str, META_TABLE: str, TARGET_SCHEMA: str):

    # Step 1: Pull all PII columns from metadata table
    query = f"""
        SELECT table_name, column_name
        FROM {META_SCHEMA}.{META_TABLE}
        WHERE UPPER(PII_flag) = 'Y'
          AND UPPER(schema_name) = UPPER('{TARGET_SCHEMA}')
        ORDER BY table_name, column_name
    """
    pii_refs = session.sql(query).collect()

    if not pii_refs:
        return session.create_dataframe(
            [Row(TABLE_NAME="*", COLUMN_VALUES=f"No PII columns found in metadata for schema {TARGET_SCHEMA}")]
        )

    results = []
    tables = {}
    for row in pii_refs:
        tables.setdefault(row.TABLE_NAME.upper(), []).append(row.COLUMN_NAME.upper())

    # Step 2: For each table, query PII columns
    for table, cols in tables.items():
        col_list = ", ".join(cols)
        sql_stmt = f"SELECT {col_list} FROM {TARGET_SCHEMA}.{table} LIMIT 10"
        try:
            df = session.sql(sql_stmt).collect()
            for record in df:
                results.append(Row(TABLE_NAME=table, COLUMN_VALUES=record.as_dict()))
        except Exception as e:
            results.append(Row(TABLE_NAME=table, COLUMN_VALUES=f"Error querying table: {str(e)}"))

    return session.create_dataframe(results)
$$;


-- Example metadata table (META_DB.META.META_PII) with columns:
-- schema_name | table_name | column_name | pii_flag

-- Run test for schema PUBLIC using metadata
CALL TEST_PII_FROM_META('META', 'META_PII', 'PUBLIC');
