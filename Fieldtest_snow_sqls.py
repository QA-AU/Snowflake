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


CALL run_sql($$
DROP TABLE IF EXISTS META_TABLE_MAP
$$);

CALL run_sql($$
CREATE OR REPLACE TABLE META_TABLE_MAP (
  RULE_ID            NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  S_SCHEMA           STRING         NOT NULL,
  S_TABLE            STRING         NOT NULL,
  S_IGNORE_COLUMNS   VARIANT,     
  T_SCHEMA           STRING         NOT NULL,
  T_TABLE            STRING         NOT NULL,
  T_IGNORE_COLUMNS   VARIANT,      
  IS_ACTIVE          BOOLEAN        DEFAULT TRUE,
  NOTE               STRING,
  CREATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
$$);

CALL run_sql($$
INSERT INTO META_TABLE_MAP
  (S_SCHEMA, S_TABLE, S_IGNORE_COLUMNS, T_SCHEMA, T_TABLE, T_IGNORE_COLUMNS, NOTE)
SELECT
  'RAW', 'CUSTOMERS', PARSE_JSON('["LOAD_TS","_INGEST_ID"]'),
  'DW',  'DIM_CUSTOMER', PARSE_JSON('["_ETL_TS","HASHDIFF"]'),
  'Ignore ETL/system columns'
$$);




CALL run_sql('SELECT * from  META_TABLE_MAP');
 