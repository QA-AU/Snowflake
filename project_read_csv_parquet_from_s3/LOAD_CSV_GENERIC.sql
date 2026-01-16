/* ============================================================================
 PROCEDURE NAME  : STG.LOAD_CSV_GENERIC
 VERSION         : 1.3.0
 CREATED ON      : 2026-01-16

 PURPOSE
 -------
 Generic, defensive CSV ingestion stored procedure using Snowpark Python.
 Loads ONE CSV file at a time from an external stage.

 HARD RULE
 ---------
 On EVERY execution this procedure:
   - DROPS
   - RECREATES
   - RELOADS

 the following tables:
   - STG.<USER_PREFIX>_RAW_CSV_LANDING
   - STG.<USER_PREFIX>_CSV_LOAD_TELEMETRY
   - <TARGET_TABLE>

 Designed for deterministic reruns (QA / migration / reconciliation).

 PARAMETERS
 ----------
 USER_PREFIX   : Prefix to isolate working tables
 STAGE_PATH    : External stage path to ONE CSV file
 TARGET_TABLE  : Final target table (WILL BE DROPPED)
 DELIMITER     : Column delimiter (e.g. '¿', '|', ',', ';')
 HAS_HEADER    : TRUE if header is first row
 HEADER_LIST   : Optional comma-separated header list

 RETURN VALUE
 ------------
 VARIANT with run_id, records_loaded, status
 ============================================================================ */

CREATE OR REPLACE PROCEDURE STG.LOAD_CSV_GENERIC(
    USER_PREFIX STRING,
    STAGE_PATH STRING,
    TARGET_TABLE STRING,
    DELIMITER STRING,
    HAS_HEADER BOOLEAN,
    HEADER_LIST STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, split, size, row_number, lit
)
from snowflake.snowpark.window import Window
import uuid

def run(session: Session,
        user_prefix: str,
        stage_path: str,
        target_table: str,
        delimiter: str,
        has_header: bool,
        header_list: str):

    run_id = str(uuid.uuid4())

    raw_table = f"STG.{user_prefix}_RAW_CSV_LANDING"
    telemetry_table = f"STG.{user_prefix}_CSV_LOAD_TELEMETRY"

    header_list = header_list.strip() if header_list and header_list.strip() else None
    delim_sql = delimiter.replace("'", "''")

    # --------------------------------------------------
    # DROP TABLES (ALWAYS)
    # --------------------------------------------------
    session.sql(f"DROP TABLE IF EXISTS {raw_table}").collect()
    session.sql(f"DROP TABLE IF EXISTS {telemetry_table}").collect()
    session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()

    # --------------------------------------------------
    # CREATE TABLES
    # --------------------------------------------------
    session.sql(f"""
        CREATE TABLE {raw_table} (
            file_name STRING,
            raw_row STRING,
            load_ts TIMESTAMP
        )
    """).collect()

    session.sql(f"""
        CREATE TABLE {telemetry_table} (
            run_id STRING,
            event_type STRING,
            file_path STRING,
            target_table STRING,
            header_present BOOLEAN,
            record_count NUMBER,
            status STRING,
            sample_row_1 STRING,
            sample_row_2 STRING,
            headers ARRAY,
            event_ts TIMESTAMP
        )
    """).collect()

    session.sql(f"""
        CREATE TABLE {target_table} (
            file_name STRING,
            headers ARRAY,
            data ARRAY,
            load_ts TIMESTAMP
        )
    """).collect()

    # --------------------------------------------------
    # START TELEMETRY (11 columns)
    # --------------------------------------------------
    session.create_dataframe(
        [(
            run_id, "START", stage_path, target_table,
            has_header, None, "RUNNING",
            None, None, None, None
        )],
        schema=[
            "RUN_ID","EVENT_TYPE","FILE_PATH","TARGET_TABLE",
            "HEADER_PRESENT","RECORD_COUNT","STATUS",
            "SAMPLE_ROW_1","SAMPLE_ROW_2","HEADERS","EVENT_TS"
        ]
    ).write.mode("append").save_as_table(telemetry_table)

    # --------------------------------------------------
    # LOAD RAW CSV (ENTIRE LINE)
    # --------------------------------------------------
    session.sql(f"""
        COPY INTO {raw_table} (file_name, raw_row, load_ts)
        FROM (
            SELECT METADATA$FILENAME, $1, CURRENT_TIMESTAMP
            FROM {stage_path}
        )
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = '\\u0001'
            RECORD_DELIMITER = '\\n'
            SKIP_HEADER = 0
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
    """).collect()

    df = session.table(raw_table)

    # --------------------------------------------------
    # ROW NUMBER PER FILE
    # --------------------------------------------------
    w = Window.partition_by(col("file_name")).order_by(col("load_ts"))
    df = df.with_column("rn", row_number().over(w))

    # --------------------------------------------------
    # SAMPLE RAW ROWS
    # --------------------------------------------------
    samples = df.select("raw_row").limit(2).collect()
    sample1 = samples[0][0] if samples else None
    sample2 = samples[1][0] if len(samples) > 1 else None

    # --------------------------------------------------
    # HEADER RESOLUTION
    # --------------------------------------------------
    if header_list:
        headers = [h.strip() for h in header_list.split(",")]

    elif has_header:
        headers = (
            df.filter(col("rn") == 1)
              .select(split(col("raw_row"), lit(delimiter)).alias("hdr"))
              .collect()[0]["HDR"]
        )
        df = df.filter(col("rn") > 1)

    else:
        inferred = (
            df.with_column("tmp", split(col("raw_row"), lit(delimiter)))
              .group_by(size(col("tmp")).alias("cnt"))
              .count()
              .order_by(col("count").desc())
              .limit(1)
              .collect()[0]["CNT"]
        )
        headers = [f"COL_{i+1}" for i in range(inferred)]

    # --------------------------------------------------
    # CLEAN USING FLATTEN + ARRAY_AGG (LEGACY SAFE)
    # --------------------------------------------------
    cleaned_df = session.sql(f"""
        SELECT
            t.file_name,
            ARRAY_AGG(
                CASE
                    WHEN f.value IS NULL
                      OR f.value::STRING = ''
                      OR UPPER(f.value::STRING) = 'NULL'
                    THEN NULL
                    ELSE REGEXP_REPLACE(f.value::STRING, '^"|"$', '')
                END
            ) WITHIN GROUP (ORDER BY f.index) AS data,
            CURRENT_TIMESTAMP AS load_ts
        FROM {raw_table} t,
             LATERAL FLATTEN(input => SPLIT(t.raw_row, '{delim_sql}')) f
        GROUP BY t.file_name
    """)

    # --------------------------------------------------
    # FINAL LOAD (MATCHES 4 COLUMNS)
    # --------------------------------------------------
    final_df = cleaned_df.select(
        col("file_name"),
        lit(headers).alias("headers"),
        col("data"),
        col("load_ts")
    )

    final_df.write.mode("append").save_as_table(target_table)
    record_count = final_df.count()

    # --------------------------------------------------
    # END TELEMETRY (11 columns)
    # --------------------------------------------------
    session.create_dataframe(
        [(
            run_id, "END", stage_path, target_table,
            has_header, record_count, "SUCCESS",
            sample1, sample2, headers, None
        )],
        schema=[
            "RUN_ID","EVENT_TYPE","FILE_PATH","TARGET_TABLE",
            "HEADER_PRESENT","RECORD_COUNT","STATUS",
            "SAMPLE_ROW_1","SAMPLE_ROW_2","HEADERS","EVENT_TS"
        ]
    ).write.mode("append").save_as_table(telemetry_table)

    return {
        "version": "1.3.0",
        "run_id": run_id,
        "records_loaded": record_count,
        "status": "SUCCESS"
    }
$$;
