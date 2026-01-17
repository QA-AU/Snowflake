DOES NOT WORKS

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
    col, split, lit, row_number
)
from snowflake.snowpark.window import Window
import uuid, json

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

    # ------------------------------------------------------------------
    # DROP TABLES (ALWAYS – deterministic reruns)
    # ------------------------------------------------------------------
    session.sql(f"DROP TABLE IF EXISTS {raw_table}").collect()
    session.sql(f"DROP TABLE IF EXISTS {telemetry_table}").collect()
    session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()

    # ------------------------------------------------------------------
    # CREATE TABLES (NO DEFAULTS RELIED UPON)
    # ------------------------------------------------------------------
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
            event_ts TIMESTAMP
        )
    """).collect()

    session.sql(f"""
        CREATE TABLE {target_table} (
            file_name STRING,
            headers ARRAY,
            row_data VARIANT,
            load_ts TIMESTAMP
        )
    """).collect()

    # ------------------------------------------------------------------
    # TELEMETRY START
    # ------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO {telemetry_table}
        VALUES (
            '{run_id}',
            'START',
            '{stage_path.replace("'", "''")}',
            '{target_table}',
            {str(has_header).upper()},
            NULL,
            'RUNNING',
            CURRENT_TIMESTAMP
        )
    """).collect()

    # ------------------------------------------------------------------
    # LOAD RAW CSV (entire line preserved)
    # Dummy FIELD_DELIMITER avoids delimiter conflict
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # ROW NUMBER (for header extraction)
    # ------------------------------------------------------------------
    w = Window.partition_by(col("file_name")).order_by(col("load_ts"))
    df = df.with_column("rn", row_number().over(w))

    # ------------------------------------------------------------------
    # HEADER RESOLUTION
    # ------------------------------------------------------------------
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
        col_count = (
            df.select(split(col("raw_row"), lit(delimiter)).alias("cols"))
              .limit(1)
              .collect()[0]["COLS"]
        )
        headers = [f"COL_{i+1}" for i in range(len(col_count))]

    headers_json = json.dumps(headers).replace("'", "''")

    # ------------------------------------------------------------------
    # PARSE ROW → VARIANT (NO AGGREGATION)
    # ------------------------------------------------------------------
    parsed_df = df.with_column(
        "row_data",
        split(col("raw_row"), lit(delimiter))
    ).select(
        col("file_name"),
        lit(headers).alias("headers"),
        col("row_data"),
        col("load_ts")
    )

    parsed_df.write.mode("append").save_as_table(target_table)

    record_count = parsed_df.count()

    # ------------------------------------------------------------------
    # TELEMETRY END
    # ------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO {telemetry_table}
        VALUES (
            '{run_id}',
            'END',
            '{stage_path.replace("'", "''")}',
            '{target_table}',
            {str(has_header).upper()},
            {record_count},
            'SUCCESS',
            CURRENT_TIMESTAMP
        )
    """).collect()

    return {
        "version": "2.0.0",
        "run_id": run_id,
        "records_loaded": record_count,
        "status": "SUCCESS"
    }
$$;
