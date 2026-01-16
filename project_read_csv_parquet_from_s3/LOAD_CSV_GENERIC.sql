/* ============================================================================
 PROCEDURE NAME  : STG.LOAD_CSV_GENERIC
 VERSION         : 1.3.1
 CREATED ON      : 2026-01-16

 HARD RULE
 ---------
 ALWAYS DROP + CREATE:
   - STG.<USER_PREFIX>_RAW_CSV_LANDING
   - STG.<USER_PREFIX>_CSV_LOAD_TELEMETRY
   - <TARGET_TABLE>

 PARAMETERS
 ----------
 USER_PREFIX   : Prefix to isolate working tables
 STAGE_PATH    : External stage path to ONE CSV file
 TARGET_TABLE  : Final target table (WILL BE DROPPED)
 DELIMITER     : Column delimiter (e.g. '¿', '|', ',', ';')
 HAS_HEADER    : TRUE if header is first row
 HEADER_LIST   : Optional comma-separated header list
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
from snowflake.snowpark.functions import col, split, size, row_number, lit
from snowflake.snowpark.window import Window
import uuid, json

def _sql_str(s: str) -> str:
    """Escape a Python string for safe embedding into a Snowflake SQL string literal."""
    if s is None:
        return "NULL"
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"

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
    delim_sql = delimiter.replace("'", "''")  # used inside SQL literal

    # --------------------------------------------------
    # DROP TABLES (ALWAYS)
    # --------------------------------------------------
    session.sql(f"DROP TABLE IF EXISTS {raw_table}").collect()
    session.sql(f"DROP TABLE IF EXISTS {telemetry_table}").collect()
    session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()

    # --------------------------------------------------
    # CREATE TABLES (NO DEFAULTS RELIED UPON)
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
    # TELEMETRY START (SQL INSERT, explicit casts)
    # --------------------------------------------------
    session.sql(f"""
        INSERT INTO {telemetry_table} (
            run_id, event_type, file_path, target_table, header_present,
            record_count, status, sample_row_1, sample_row_2, headers, event_ts
        )
        SELECT
            {_sql_str(run_id)}            AS run_id,
            'START'                       AS event_type,
            {_sql_str(stage_path)}        AS file_path,
            {_sql_str(target_table)}      AS target_table,
            {str(bool(has_header)).upper()} AS header_present,
            NULL                          AS record_count,
            'RUNNING'                     AS status,
            NULL                          AS sample_row_1,
            NULL                          AS sample_row_2,
            CAST(PARSE_JSON('[]') AS ARRAY) AS headers,
            CURRENT_TIMESTAMP             AS event_ts
    """).collect()

    # --------------------------------------------------
    # LOAD RAW CSV (entire line in $1)
    # FIELD_DELIMITER must differ from RECORD_DELIMITER -> use dummy \\u0001
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
    # ROW NUMBER PER FILE (for header extraction)
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

    # Prepare JSON for headers as ARRAY
    headers_json = json.dumps(headers)  # e.g. ["id","name",...]
    headers_json_sql = headers_json.replace("'", "''")  # escape for SQL literal

    # --------------------------------------------------
    # CLEAN USING FLATTEN + ARRAY_AGG WITHIN GROUP (ORDER BY index)
    # Produces an ARRAY called data
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
    # FINAL LOAD: match target schema exactly (4 cols)
    # headers is forced to ARRAY via CAST(PARSE_JSON(...) AS ARRAY)
    # --------------------------------------------------
    final_df = cleaned_df.select(
        col("file_name"),
        lit(None).alias("headers_placeholder"),  # replaced via SQL below
        col("data"),
        col("load_ts")
    )

    # Write final_df into a temp table, then insert into target with correct headers ARRAY
    tmp_final = f"STG.{user_prefix}_TMP_FINAL_{run_id.replace('-', '_')}"
    session.sql(f"DROP TABLE IF EXISTS {tmp_final}").collect()
    final_df.write.mode("overwrite").save_as_table(tmp_final)

    session.sql(f"""
        INSERT INTO {target_table} (file_name, headers, data, load_ts)
        SELECT
            file_name,
            CAST(PARSE_JSON('{headers_json_sql}') AS ARRAY) AS headers,
            data,
            load_ts
        FROM {tmp_final}
    """).collect()

    # Count loaded records
    record_count = session.table(target_table).count()

    # Cleanup temp
    session.sql(f"DROP TABLE IF EXISTS {tmp_final}").collect()

    # --------------------------------------------------
    # TELEMETRY END (SQL INSERT, explicit headers ARRAY)
    # --------------------------------------------------
    session.sql(f"""
        INSERT INTO {telemetry_table} (
            run_id, event_type, file_path, target_table, header_present,
            record_count, status, sample_row_1, sample_row_2, headers, event_ts
        )
        SELECT
            {_sql_str(run_id)}              AS run_id,
            'END'                           AS event_type,
            {_sql_str(stage_path)}          AS file_path,
            {_sql_str(target_table)}        AS target_table,
            {str(bool(has_header)).upper()} AS header_present,
            {record_count}                  AS record_count,
            'SUCCESS'                       AS status,
            {_sql_str(sample1)}             AS sample_row_1,
            {_sql_str(sample2)}             AS sample_row_2,
            CAST(PARSE_JSON('{headers_json_sql}') AS ARRAY) AS headers,
            CURRENT_TIMESTAMP               AS event_ts
    """).collect()

    return {
        "version": "1.3.1",
        "run_id": run_id,
        "file_path": stage_path,
        "target_table": target_table,
        "records_loaded": record_count,
        "status": "SUCCESS"
    }
$$;
