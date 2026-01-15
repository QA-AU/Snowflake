CREATE OR REPLACE PROCEDURE STG.LOAD_CSV_DYNAMIC_PY(
    stage_path STRING,          -- @DB.SCHEMA.STAGE/path/file.csv
    target_table STRING,        -- fully qualified or schema.table
    delim_char STRING,          -- ',', '|', '\t', ';', '¿', or 'AUTO'
    file_has_headers BOOLEAN,   -- TRUE if first row contains headers
    headers_json STRING,        -- JSON array, '[]', or NULL
    strict_mode BOOLEAN DEFAULT FALSE,
    sample_rows INTEGER DEFAULT 10
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import json
import re
import unicodedata
import time

# =============================================================================
# Procedure : STG.LOAD_CSV_DYNAMIC_PY
# Version   : 1.7.2
#
# Patch notes (1.7.2)
# - Introduced QUOTE_CHAR constant
# - Removed escaped quotes from COPY SQL
# - Eliminated editor / parser ambiguity
#
# Core features
# - AUTO delimiter detection (incl non-ASCII like ¿)
# - DROP + CREATE always
# - COPY fast-path (ASCII delimiters only)
# - Snowpark fallback
# - System headers when missing
# - Deterministic header normalization & dedupe
# - Row counts + rich telemetry
# =============================================================================


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
QUOTE_CHAR = '"'   # used in COPY FIELD_OPTIONALLY_ENCLOSED_BY


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def is_non_ascii(char):
    return ord(char) > 127


def detect_delimiter(sample_lines):
    candidates = [",", "|", "\t", ";", "¿"]
    scores = {}
    for d in candidates:
        scores[d] = max(len(line.split(d)) for line in sample_lines)
    best = max(scores, key=scores.get)
    return best, scores


def generate_system_headers(n):
    return [f"COL_{i}" for i in range(1, n + 1)]


# -----------------------------------------------------------------------------
# Header normalization + deterministic dedupe
# -----------------------------------------------------------------------------
def normalize_and_dedupe_headers(raw_headers):

    def normalize(col_name, pos):
        if not col_name or not col_name.strip():
            return f"COL_{pos}"

        col_name = unicodedata.normalize("NFKD", col_name)
        col_name = col_name.encode("ascii", "ignore").decode()
        col_name = col_name.upper()
        col_name = re.sub(r"[^A-Z0-9_]", "_", col_name)
        col_name = re.sub(r"_+", "_", col_name).strip("_")

        if re.match(r"^[0-9]", col_name):
            col_name = f"COL_{col_name}"

        return col_name

    seen, final_headers, duplicates = {}, [], []

    for i, raw in enumerate(raw_headers, start=1):
        base = normalize(raw, i)
        if base not in seen:
            seen[base] = 1
            final_headers.append(base)
        else:
            seen[base] += 1
            final_headers.append(f"{base}_{seen[base]}")
            duplicates.append(base)

    return final_headers, sorted(set(duplicates))


# -----------------------------------------------------------------------------
# DROP + CREATE target table
# -----------------------------------------------------------------------------
def recreate_target_table(session, target_table, columns):
    session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()
    cols_sql = ", ".join(f"{c} STRING" for c in columns)
    session.sql(f"CREATE TABLE {target_table} ({cols_sql})").collect()


# -----------------------------------------------------------------------------
# Build COPY SQL safely (NO escaped quotes)
# -----------------------------------------------------------------------------
def build_copy_sql(stage_path, target_table, delim):
    if delim == "'":
        raise ValueError("Single-quote delimiter is not supported")

    sql = (
        f"COPY INTO {target_table} "
        f"FROM {stage_path} "
        f"FILE_FORMAT = ( "
        f"TYPE = CSV "
        f"FIELD_DELIMITER = '{delim}' "
        f"SKIP_HEADER = 1 "
        f"FIELD_OPTIONALLY_ENCLOSED_BY = '{QUOTE_CHAR}' "
        f"ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE "
        f")"
    )
    return sql


def mask_copy_sql(sql):
    return re.sub(r"FROM\s+@\S+", "FROM @<STAGE_PATH>", sql)


# -----------------------------------------------------------------------------
# COPY fast-path
# -----------------------------------------------------------------------------
def try_copy(session, copy_sql):
    start = time.time()
    try:
        result = session.sql(copy_sql).collect()
        rows = sum(r["rows_loaded"] for r in result)
        return True, round(time.time() - start, 3), rows, None
    except Exception as e:
        return False, round(time.time() - start, 3), 0, str(e)


# -----------------------------------------------------------------------------
# Snowpark fallback loader
# -----------------------------------------------------------------------------
def snowpark_fallback(session, raw_df, target_table, headers, delim, file_has_headers):
    df = raw_df.select(col("VALUE").split(delim).alias("cols"))

    if file_has_headers:
        df = df.filter(df["cols"] != headers)

    for i, c in enumerate(headers):
        df = df.with_column(c, df["cols"][i])

    df = df.select(headers)

    start = time.time()
    df.write.mode("append").save_as_table(target_table)
    count = session.table(target_table).count()
    return round(time.time() - start, 3), count


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def run(
    session: Session,
    stage_path: str,
    target_table: str,
    delim_char: str,
    file_has_headers: bool,
    headers_json: str,
    strict_mode: bool,
    sample_rows: int
):

    telemetry = {
        "system_headers_generated": False
    }

    raw_df = session.read.text(stage_path)

    sample = raw_df.limit(sample_rows).collect()
    if not sample:
        return {"status": "FAIL", "reason": "File readable but empty", "rows_loaded": 0}

    sample_lines = [r["VALUE"] for r in sample]

    # Delimiter resolution
    if delim_char.upper() == "AUTO":
        delim, scores = detect_delimiter(sample_lines)
        telemetry["delimiter_detection"] = scores
    else:
        delim = delim_char

    telemetry["delimiter_used"] = delim

    split_rows = [line.split(delim) for line in sample_lines]
    max_cols = max(len(r) for r in split_rows)
    if max_cols <= 1:
        return {
            "status": "FAIL",
            "reason": "Delimiter did not split columns",
            "delimiter_used": delim,
            "rows_loaded": 0
        }

    # Header resolution
    if file_has_headers:
        raw_headers = split_rows[0]
    else:
        if headers_json and headers_json.strip() not in ("", "[]"):
            raw_headers = json.loads(headers_json)
        else:
            raw_headers = generate_system_headers(max_cols)
            telemetry["system_headers_generated"] = True

    final_headers, duplicates = normalize_and_dedupe_headers(raw_headers)

    if strict_mode and duplicates:
        return {
            "status": "FAIL",
            "reason": "Duplicate headers detected",
            "duplicates": duplicates,
            "delimiter_used": delim,
            "rows_loaded": 0
        }

    recreate_target_table(session, target_table, final_headers)

    # COPY eligibility
    if not is_non_ascii(delim):
        copy_sql = build_copy_sql(stage_path, target_table, delim)
        telemetry["copy_sql_masked"] = mask_copy_sql(copy_sql)
        telemetry["copy_attempted"] = True

        ok, t_sec, rows, err = try_copy(session, copy_sql)
        telemetry["copy_duration_sec"] = t_sec

        if ok:
            telemetry["copy_success"] = True
            telemetry["rows_loaded"] = rows
            return {
                "status": "SUCCESS",
                "load_method": "COPY_FAST_PATH",
                "rows_loaded": rows,
                "telemetry": telemetry
            }

        telemetry["copy_success"] = False
        telemetry["copy_error"] = err

    else:
        telemetry["copy_attempted"] = False
        telemetry["copy_skipped_reason"] = "NON_ASCII_DELIMITER"

    # Fallback
    fb_time, fb_rows = snowpark_fallback(
        session, raw_df, target_table, final_headers, delim, file_has_headers
    )

    telemetry["fallback_used"] = True
    telemetry["fallback_duration_sec"] = fb_time
    telemetry["rows_loaded"] = fb_rows

    return {
        "status": "SUCCESS",
        "load_method": "SNOWPARK_FALLBACK",
        "rows_loaded": fb_rows,
        "telemetry": telemetry
    }
$$;



-- -- SAMPLE CALLERS & USER OPTIONS
-- 1️⃣ File with headers, delimiter known
-- CALL STG.LOAD_CSV_DYNAMIC_PY(
--   '@RAW_DB.STG_EXT/data/file.csv',
--   'STG.MY_TABLE',
--   '|',
--   TRUE,
--   NULL,
--   FALSE,
--   10
-- );

-- 2️⃣ File with headers, delimiter unknown
-- CALL STG.LOAD_CSV_DYNAMIC_PY(
--   '@RAW_DB.STG_EXT/data/file.csv',
--   'STG.MY_TABLE',
--   'AUTO',
--   TRUE,
--   NULL,
--   FALSE,
--   10
-- );

-- 3️⃣ File without headers, headers supplied
-- CALL STG.LOAD_CSV_DYNAMIC_PY(
--   '@RAW_DB.STG_EXT/data/file.csv',
--   'STG.MY_TABLE',
--   ',',
--   FALSE,
--   '["ID","NAME","AGE"]',
--   FALSE,
--   10
-- );

-- 4️⃣ File without headers, no JSON → system headers
-- CALL STG.LOAD_CSV_DYNAMIC_PY(
--   '@RAW_DB.STG_EXT/data/file.csv',
--   'STG.MY_TABLE',
--   'AUTO',
--   FALSE,
--   NULL,
--   FALSE,
--   10
-- );


-- Telemetry output will include:

-- "system_headers_generated": true

-- 5️⃣ Strict mode (fail on duplicate headers)
-- CALL STG.LOAD_CSV_DYNAMIC_PY(
--   '@RAW_DB.STG_EXT/data/file.csv',
--   'STG.MY_TABLE',
--   '|',
--   TRUE,
--   NULL,
--   TRUE,
--   10
-- );