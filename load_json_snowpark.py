# ============================================================
#  Snowpark Python Worksheet
#  Paste this entire file into your worksheet and run it.
#  Session is provided automatically by the worksheet runtime.
# ============================================================

import json
import re
import sys
from datetime import datetime, timezone
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException


# ============================================================
#  LOAD_JSON_FROM_STAGE  – core function
#  Takes the active session + your variables, returns a JSON
#  commentary string with debug log embedded.
# ============================================================

def load_json_from_stage(
    session         : Session,
    stage_file_path : str,
    target_table    : str,
    headers         : str,         # comma-separated e.g. "id, name, dept" or "" / None → auto-detect
) -> dict:

    # ── debug logger ─────────────────────────────────────────
    debug_log   = []
    step_number = [0]

    def log(label: str, detail: str = "", data=None):
        step_number[0] += 1
        ts    = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        entry = {"step": step_number[0], "time": ts, "label": label, "detail": detail}
        if data is not None:
            entry["data"] = data
        debug_log.append(entry)
        print(f"[{step_number[0]:02d}] [{ts}] {label} | {detail}")
        if data is not None:
            print(f"       ↳ {json.dumps(data)}")

    def fail(msg: str) -> dict:
        log("FAILED", msg)
        return {
            "file_path"   : stage_file_path,
            "rows_read"   : 0,
            "rows_loaded" : 0,
            "headers_used": [],
            "sample_table": sample_table,
            "sample_row"  : None,
            "status"      : "FAILED",
            "error"       : msg,
            "debug_log"   : debug_log,
        }

    def run_sql(label: str, sql: str, description: str = ""):
        log(label, description, {"sql_preview": sql.strip()[:200]})
        try:
            result = session.sql(sql).collect()
            log(f"{label} ✓", f"Returned {len(result)} row(s)")
            return result
        except SnowparkSQLException as e:
            log(f"{label} ✗", str(e))
            raise

    # ── helpers ───────────────────────────────────────────────
    def quote(name: str) -> str:
        return f'"{name.upper()}"'

    def safe_col(name: str) -> str:
        return re.sub(r'[^A-Za-z0-9_]', '_', name.strip())

    def split_table(full_name: str):
        parts = full_name.strip().split('.')
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        raise ValueError(f"TARGET_TABLE must be SCHEMA.TABLE, got: {full_name}")

    # ── Step 0: parse headers string + resolve names ─────────
    # Accepts:  "id, name, dept"  →  ["id", "name", "dept"]
    # Accepts:  ""  or  None      →  []  (triggers auto-detect)
    if headers and str(headers).strip():
        raw_header_str = str(headers).strip()
        parsed_headers = [h.strip() for h in raw_header_str.split(",") if h.strip()]
    else:
        raw_header_str = ""
        parsed_headers = []

    log("INIT", "Parsing inputs", {
        "stage_file_path"  : stage_file_path,
        "target_table"     : target_table,
        "headers_raw_input": raw_header_str if raw_header_str else "(empty — auto-detect)",
        "headers_parsed"   : parsed_headers,
    })

    try:
        schema_name, table_name = split_table(target_table)
    except ValueError as e:
        sample_table = "N/A"
        return fail(str(e))

    sample_table = f"{schema_name}.LOAD_SAMPLE_{table_name}"
    temp_raw     = f"{schema_name}._TMP_RAW_{table_name}"
    fmt_name     = f"{schema_name}._FMT_{table_name}"

    log("NAMES RESOLVED", "Derived object names", {
        "schema"        : schema_name,
        "target_table"  : target_table,
        "sample_table"  : sample_table,
        "temp_raw_table": temp_raw,
        "file_format"   : fmt_name,
    })

    # ── Step 1: create file format (STRIP_OUTER_ARRAY=FALSE first) ──
    def create_fmt(strip: bool):
        strip_str = 'TRUE' if strip else 'FALSE'
        run_sql(
            "CREATE FILE FORMAT",
            f"""
            CREATE OR REPLACE FILE FORMAT {fmt_name}
                TYPE               = 'JSON'
                STRIP_OUTER_ARRAY  = {strip_str}
                IGNORE_UTF8_ERRORS = TRUE
                TRIM_SPACE         = TRUE
                NULL_IF            = ('NULL', 'null', '')
            """,
            f"STRIP_OUTER_ARRAY = {strip_str}"
        )

    try:
        create_fmt(strip=False)
    except SnowparkSQLException as e:
        return fail(f"Could not create file format: {e}")

    # ── Step 2: create temp table + probe COPY ────────────────
    try:
        run_sql("CREATE TEMP TABLE",
                f"CREATE OR REPLACE TEMP TABLE {temp_raw} (RAW_JSON VARIANT)",
                "Scratch table for raw VARIANT rows")

        run_sql("COPY INTO (probe)",
                f"""
                COPY INTO {temp_raw} (RAW_JSON)
                FROM (SELECT $1 FROM {stage_file_path})
                FILE_FORMAT = (FORMAT_NAME = '{fmt_name}')
                ON_ERROR    = 'CONTINUE'
                PURGE       = FALSE
                """,
                "First pass — STRIP_OUTER_ARRAY=FALSE to probe file structure")
    except SnowparkSQLException as e:
        return fail(f"Initial COPY INTO failed: {e}")

    # ── Step 3: probe TYPEOF to decide STRIP_OUTER_ARRAY ─────
    try:
        probe = run_sql("PROBE TYPEOF",
                        f"""
                        SELECT COUNT(*)              AS CNT,
                               TYPEOF(MIN(RAW_JSON)) AS FIRST_TYPE
                        FROM   {temp_raw}
                        """,
                        "Check row count + type of first element")
    except SnowparkSQLException as e:
        return fail(f"Probe query failed: {e}")

    row_count  = probe[0][0]
    first_type = (probe[0][1] or "").upper()

    #  ┌─────────┬────────────┬──────────────────────────────────┐
    #  │  Count  │  TYPEOF    │  Decision                        │
    #  ├─────────┼────────────┼──────────────────────────────────┤
    #  │    1    │  ARRAY     │  Re-COPY STRIP_OUTER_ARRAY=TRUE  │
    #  │    1    │  OBJECT    │  Keep   STRIP_OUTER_ARRAY=FALSE  │
    #  │   >1    │  OBJECT    │  Keep   STRIP_OUTER_ARRAY=FALSE  │
    #  └─────────┴────────────┴──────────────────────────────────┘
    strip_outer   = (row_count == 1 and first_type == "ARRAY")
    strip_applied = "TRUE" if strip_outer else "FALSE"

    log("PROBE RESULT", "STRIP_OUTER_ARRAY decision", {
        "row_count_after_probe": row_count,
        "typeof_first_row"     : first_type,
        "strip_outer_array"    : strip_applied,
        "reason": "Wrapped JSON array — will re-COPY" if strip_outer
                  else "NDJSON / single object — no re-copy needed",
    })

    # ── Step 4: re-COPY if wrapped JSON array ────────────────
    if strip_outer:
        try:
            create_fmt(strip=True)
            run_sql("TRUNCATE TEMP TABLE",
                    f"TRUNCATE TABLE {temp_raw}",
                    "Clear probe rows before definitive COPY")
            run_sql("COPY INTO (final)",
                    f"""
                    COPY INTO {temp_raw} (RAW_JSON)
                    FROM (SELECT $1 FROM {stage_file_path})
                    FILE_FORMAT = (FORMAT_NAME = '{fmt_name}')
                    ON_ERROR    = 'CONTINUE'
                    PURGE       = FALSE
                    """,
                    "Definitive COPY with STRIP_OUTER_ARRAY=TRUE")
        except SnowparkSQLException as e:
            return fail(f"Re-COPY with STRIP_OUTER_ARRAY=TRUE failed: {e}")

    # ── Step 5: count valid rows ──────────────────────────────
    try:
        total_read = run_sql(
            "COUNT VALID ROWS",
            f"SELECT COUNT(*) FROM {temp_raw} WHERE RAW_JSON IS NOT NULL",
            "Count non-null rows after COPY"
        )[0][0]
    except SnowparkSQLException as e:
        return fail(f"Row count query failed: {e}")

    log("ROWS READ", f"{total_read} valid row(s) found after COPY")

    if total_read == 0:
        return fail("No non-null rows found in stage file after COPY")

    # ── Step 6: determine columns ─────────────────────────────
    use_custom = bool(parsed_headers)

    if use_custom:
        col_names      = [safe_col(h) for h in parsed_headers]
        headers_source = "CALLER_PROVIDED"
        log("COLUMNS", f"{len(col_names)} caller-provided headers", {"columns": col_names})
    else:
        log("COLUMNS", "Headers empty/null — deriving keys from first JSON row")
        try:
            first_val  = run_sql("FETCH FIRST ROW",
                                 f"SELECT RAW_JSON FROM {temp_raw} LIMIT 1",
                                 "Read one row to extract top-level keys")[0][0]
            first_obj  = json.loads(str(first_val)) if isinstance(first_val, str) \
                         else json.loads(first_val)
            col_names  = [safe_col(k) for k in first_obj.keys()]
            headers_source = "DERIVED_FROM_JSON"
            log("COLUMNS DERIVED", f"{len(col_names)} column(s) found", {"columns": col_names})
        except Exception as e:
            return fail(f"Could not derive columns from first JSON row: {e}")

    if not col_names:
        return fail("No columns could be determined")

    # ── Step 7: create target table ───────────────────────────
    col_defs    = ",\n    ".join([f"{quote(c)} VARIANT" for c in col_names])
    quoted_cols = ", ".join([quote(c) for c in col_names])

    try:
        run_sql("CREATE TARGET TABLE",
                f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    {col_defs}
                )
                """,
                f"{len(col_names)} VARIANT columns (no deep flatten)")
    except SnowparkSQLException as e:
        return fail(f"Could not create target table: {e}")

    # ── Step 8: INSERT into target ────────────────────────────
    select_exprs = ",\n        ".join([f"RAW_JSON['{c}']" for c in col_names])

    try:
        run_sql("INSERT INTO TARGET",
                f"""
                INSERT INTO {target_table} ({quoted_cols})
                SELECT
                    {select_exprs}
                FROM {temp_raw}
                WHERE RAW_JSON IS NOT NULL
                """,
                f"Inserting top-level keys as VARIANT columns")
    except SnowparkSQLException as e:
        return fail(f"INSERT into target table failed: {e}")

    try:
        rows_loaded = run_sql("COUNT LOADED ROWS",
                              f"SELECT COUNT(*) FROM {target_table}",
                              "Verify row count in target table")[0][0]
    except SnowparkSQLException as e:
        return fail(f"Post-insert count failed: {e}")

    log("INSERT COMPLETE", f"{rows_loaded} row(s) now in {target_table}")

    # ── Step 9: create sample table ───────────────────────────
    sample_row = {}
    try:
        run_sql("CREATE SAMPLE TABLE",
                f"""
                CREATE OR REPLACE TABLE {sample_table} AS
                SELECT {quoted_cols}
                FROM   {target_table}
                LIMIT  1
                """,
                f"1-row snapshot → {sample_table}")
        sample_raw = run_sql("FETCH SAMPLE ROW",
                             f"SELECT * FROM {sample_table}",
                             "Read back the single sample row")
        sample_row = {col_names[i]: str(sample_raw[0][i])
                      for i in range(len(col_names))} if sample_raw else {}
        log("SAMPLE ROW", "Captured", sample_row)
    except SnowparkSQLException as e:
        log("SAMPLE TABLE WARN", f"Non-fatal: {e}")
        sample_row = {"warning": str(e)}

    # ── Step 10: cleanup ──────────────────────────────────────
    for sql, label in [
        (f"DROP TABLE       IF EXISTS {temp_raw}", "DROP TEMP TABLE"),
        (f"DROP FILE FORMAT IF EXISTS {fmt_name}", "DROP FILE FORMAT"),
    ]:
        try:
            run_sql(label, sql, "Cleanup transient object")
        except SnowparkSQLException as e:
            log(f"{label} WARN", f"Non-fatal: {e}")

    # ── Step 11: build commentary ─────────────────────────────
    status = "PASS" if rows_loaded >= total_read else "FAILED"

    log("COMPLETE", f"Status = {status}", {
        "rows_read"  : total_read,
        "rows_loaded": rows_loaded,
    })

    return {
        "file_path"         : stage_file_path,
        "strip_outer_array" : strip_applied,
        "rows_read"         : total_read,
        "rows_loaded"       : rows_loaded,
        "headers_source"    : headers_source,
        "headers_used"      : col_names,
        "target_table"      : target_table,
        "sample_table"      : sample_table,
        "sample_row"        : sample_row,
        "status"            : status,
        "debug_log"         : debug_log,
    }


# ============================================================
#  MAIN  – edit your variables here and run the worksheet
# ============================================================

def main(session: Session):

    # ── Your variables ────────────────────────────────────────
    stage_file_path = "@extstage/data/employees.json"
    target_table    = "MY_SCHEMA.EMPLOYEES"
    headers         = "employee_id, first_name, last_name, dept, salary"
    # headers       = ""    ← empty string to auto-detect columns from JSON
    # headers       = None  ← also triggers auto-detect

    # ── Call the function ─────────────────────────────────────
    print("=" * 60)
    print("  LOAD_JSON_FROM_STAGE")
    print("=" * 60)

    result = load_json_from_stage(
        session         = session,
        stage_file_path = stage_file_path,
        target_table    = target_table,
        headers         = headers,
    )

    # ── Print summary ─────────────────────────────────────────
    status_icon = "✅ PASS" if result["status"] == "PASS" else "❌ FAILED"
    print("\n" + "=" * 60)
    print(f"  {status_icon}")
    print(f"  File       : {result['file_path']}")
    print(f"  Rows read  : {result['rows_read']}")
    print(f"  Rows loaded: {result['rows_loaded']}")
    print(f"  Headers    : {result['headers_source']} → {result['headers_used']}")
    print(f"  Target     : {result['target_table']}")
    print(f"  Sample tbl : {result['sample_table']}")

    if result.get("sample_row"):
        print("\n  Sample row:")
        for k, v in result["sample_row"].items():
            print(f"    {k:<30}: {v}")

    if result.get("error"):
        print(f"\n  ⚠️  Error: {result['error']}")

    print("=" * 60)

    # Return the full result dict so Snowsight shows it in the output panel
    return result
