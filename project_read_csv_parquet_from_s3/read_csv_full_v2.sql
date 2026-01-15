LOAD_CSV_DYNAMIC_PY   ← public entry point (unchanged)
│
├── CSV_PROBE_PLAN_PY       (read-only, diagnostics)
├── CSV_PREPARE_TABLES_PY  (DDL only)
└── CSV_LOAD_EXECUTE_PY    (COPY + fallback + rejects)

Public Interface:

CALL STG.LOAD_CSV_DYNAMIC_PY(
  '@STAGE/path/file.csv',
  'STG.MY_TABLE',
  'AUTO',
  TRUE,
  NULL,
  FALSE,
  10
);


+-----------------------------+
| LOAD_CSV_DYNAMIC_PY (public)|
+--------------+--------------+
               |
               v
+-----------------------------+
| CSV_PROBE_PLAN_PY           |
| - sample file               |
| - detect delimiter          |
| - resolve headers           |
| - COPY eligibility          |
+--------------+--------------+
               |
               v
+-----------------------------+
| CSV_PREPARE_TABLES_PY       |
| - DROP target table         |
| - CREATE target table       |
| - CREATE reject table       |
+--------------+--------------+
               |
               v
+-----------------------------+
| CSV_LOAD_EXECUTE_PY         |
| - COPY fast-path            |
| - fallback CSV parser       |
| - validate columns          |
| - capture rejects           |
+-----------------------------+


-- Procedure 1 — PROBE & PLAN
-- Purpose

-- Read sample rows

-- Detect delimiter

-- Resolve headers (file / JSON / system)

-- Normalize + dedupe headers

-- Decide COPY eligibility

-- Returns

-- A VARIANT load plan (no data touched).

CREATE OR REPLACE PROCEDURE STG.CSV_PROBE_PLAN_PY(
    stage_path STRING,
    delim_char STRING,
    file_has_headers BOOLEAN,
    headers_json STRING,
    strict_mode BOOLEAN,
    sample_rows INTEGER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import json, re, unicodedata

QUOTE_CHAR = '"'

def detect_delimiter(lines):
    cands = [",", "|", "\t", ";", "¿"]
    scores = {d: max(len(l.split(d)) for l in lines) for d in cands}
    return max(scores, key=scores.get), scores

def normalize_headers(raw):
    seen, out, dups = {}, [], []
    for i, c in enumerate(raw, 1):
        if not c or not c.strip():
            c = f"COL_{i}"
        c = unicodedata.normalize("NFKD", c).encode("ascii","ignore").decode()
        c = re.sub(r"[^A-Z0-9_]", "_", c.upper()).strip("_")
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
            dups.append(c)
    return out, sorted(set(dups))

def run(session, stage_path, delim_char, file_has_headers, headers_json, strict_mode, sample_rows):

    rows = session.sql(
        f"SELECT $1 FROM {stage_path} LIMIT {sample_rows}"
    ).collect()
    lines = [r[0] for r in rows]

    if delim_char.upper() == "AUTO":
        delim, scores = detect_delimiter(lines)
    else:
        delim, scores = delim_char, None

    split = lines[0].split(delim)

    system_headers_generated = False
    if file_has_headers:
        raw_headers = split
    elif headers_json and headers_json.strip() not in ("", "[]"):
        raw_headers = json.loads(headers_json)
    else:
        raw_headers = [f"COL_{i}" for i in range(1, len(split)+1)]
        system_headers_generated = True

    headers, dups = normalize_headers(raw_headers)

    if strict_mode and dups:
        return {
            "status": "FAIL",
            "reason": "Duplicate headers",
            "duplicates": dups
        }

    return {
        "status": "OK",
        "delimiter": delim,
        "delimiter_scores": scores,
        "headers": headers,
        "expected_cols": len(headers),
        "copy_allowed": ord(delim) <= 127,
        "skip_header": 1 if file_has_headers else 0,
        "system_headers_generated": system_headers_generated
    }
$$;

-- Procedure 2 — PREPARE TABLES (DDL ONLY)
-- Purpose

-- DROP + CREATE target table

-- Ensure reject table exists

CREATE OR REPLACE PROCEDURE STG.CSV_PREPARE_TABLES_PY(
    target_table STRING,
    headers ARRAY
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
def run(session, target_table, headers):

    session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()

    cols = ", ".join(f"{c} STRING" for c in headers)
    session.sql(f"CREATE TABLE {target_table} ({cols})").collect()

    reject_table = f"{target_table}_REJECTS"
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {reject_table} (
            RAW_ROW VARIANT,
            ERROR_REASON STRING
        )
    """).collect()

    return "TABLES_PREPARED"
$$;


-- Procedure 3 — LOAD & VALIDATE
-- Purpose

-- Try COPY fast-path

-- Fallback to Snowpark CSV (quote-safe)

-- Validate column counts

-- Capture rejects

-- Return row counts
CREATE OR REPLACE PROCEDURE STG.CSV_LOAD_EXECUTE_PY(
    stage_path STRING,
    target_table STRING,
    plan VARIANT
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import time
from snowflake.snowpark.functions import col, array_size

QUOTE_CHAR = '"'

def run(session, stage_path, target_table, plan):

    headers = plan["headers"]
    delim = plan["delimiter"]
    skip_header = plan["skip_header"]
    expected_cols = plan["expected_cols"]

    telemetry = {}

    # --- COPY fast-path
    if plan["copy_allowed"]:
        copy_sql = (
            f"COPY INTO {target_table} FROM {stage_path} "
            f"FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER='{delim}' "
            f"SKIP_HEADER={skip_header} "
            f"FIELD_OPTIONALLY_ENCLOSED_BY='{QUOTE_CHAR}' "
            f"ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)"
        )

        t0 = time.time()
        try:
            res = session.sql(copy_sql).collect()
            rows = sum(r["rows_loaded"] for r in res)
            return {
                "load_method": "COPY_FAST_PATH",
                "rows_loaded": rows,
                "telemetry": {
                    "copy_duration_sec": round(time.time()-t0,3)
                }
            }
        except Exception as e:
            telemetry["copy_error"] = str(e)

    # --- Snowpark fallback (CSV parser)
    df = (
        session.read
        .option("field_delimiter", delim)
        .option("skip_header", skip_header)
        .option("field_optionally_enclosed_by", QUOTE_CHAR)
        .csv(stage_path)
    )

    valid = df.filter(array_size(col("value")) == expected_cols)
    reject = df.filter(array_size(col("value")) != expected_cols)

    if reject.count() > 0:
        reject.select(
            col("value").alias("RAW_ROW"),
            col("value").cast("STRING").alias("ERROR_REASON")
        ).write.mode("append").save_as_table(f"{target_table}_REJECTS")

    for i, c in enumerate(headers):
        valid = valid.with_column(c, col("value")[i])

    valid = valid.select(headers)

    t0 = time.time()
    valid.write.mode("append").save_as_table(target_table)
    rows = session.table(target_table).count()

    telemetry.update({
        "fallback_parser": "CSV",
        "fallback_duration_sec": round(time.time()-t0,3)
    })

    return {
        "load_method": "SNOWPARK_FALLBACK",
        "rows_loaded": rows,
        "telemetry": telemetry
    }
$$;

-- Orchestrator — PUBLIC PROCEDURE (UNCHANGED)

CREATE OR REPLACE PROCEDURE STG.LOAD_CSV_DYNAMIC_PY(
    stage_path STRING,
    target_table STRING,
    delim_char STRING,
    file_has_headers BOOLEAN,
    headers_json STRING,
    strict_mode BOOLEAN,
    sample_rows INTEGER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
def run(session, stage_path, target_table, delim_char, file_has_headers, headers_json, strict_mode, sample_rows):

    plan = session.call(
        "STG.CSV_PROBE_PLAN_PY",
        stage_path, delim_char, file_has_headers, headers_json, strict_mode, sample_rows
    )

    if plan["status"] != "OK":
        return plan

    session.call(
        "STG.CSV_PREPARE_TABLES_PY",
        target_table,
        plan["headers"]
    )

    result = session.call(
        "STG.CSV_LOAD_EXECUTE_PY",
        stage_path,
        target_table,
        plan
    )

    return {
        "status": "SUCCESS",
        "plan": plan,
        **result
    }
$$;


