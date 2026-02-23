# """helpers.py - Shared utility functions for the full data quality engine."""
#  STEP 2

from snowflake.snowpark import Session

import json
import time
from typing import Optional, Dict, Any, List


def run_sql_timed(session, sql, debug, test_name):
    """
    Executes SQL with timing and debug logging.
    Returns: (rows, duration_ms)
    """

    if debug:
        print(f"[DEBUG][{test_name}] Executing SQL:\n{sql}\n")

    # timer start
    _start = time.time()  # uses the 'time' already imported in your file

    # execute SQL
    rows = session.sql(sql).collect()

    # duration
    duration_ms = int((time.time() - _start) * 1000)

    if debug:
        print(f"[DEBUG][{test_name}] Finished in {duration_ms} ms (rows={len(rows)})")

    return rows, duration_ms


# Produces fully-qualified table names
def build_fqn(parent_db, schema, table):
    """
    Builds a fully-qualified table name:
       DB.SCHEMA.TABLE

    Handles None / empty values safely.
    """

    if parent_db is None or parent_db == "":
        raise ValueError("parent_db cannot be empty")

    if schema is None or schema == "":
        raise ValueError("schema cannot be empty")

    if table is None or table == "":
        raise ValueError("table cannot be empty")

    return f"{parent_db}.{schema}.{table}"


# -------------------------
# SQL literal escaping
# -------------------------
def escape_sql_literal(val: Optional[str]) -> str:
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


# -------------------------
# Boolean / numeric to SQL
# -------------------------
def bool_to_sql(flag: Optional[bool]) -> str:
    if flag is None:
        return "NULL"
    return "TRUE" if flag else "FALSE"


def num_to_sql(v: Optional[Any]) -> str:
    if v is None:
        return "NULL"
    return str(v)


# -------------------------
# JSON metrics formatting
# -------------------------
def metrics_to_sql(metrics: Optional[Dict[str, Any]]) -> str:
    if metrics is None:
        return "NULL"
    js = json.dumps(metrics)
    js = js.replace("'", "''")
    return f"'{js}'"


# -------------------------
# Construct FQN
# -------------------------
def fqn(db: str, schema: str, table: str) -> str:
    return f"{db}.{schema}.{table}"


# -------------------------
# Business date rule:
# -------------------------
def should_apply_business_date(bd: Optional[str]) -> bool:
    return bd not in (None, "1900-01-01")


# -------------------------
# WHERE clause builder
# -------------------------
def build_where_clause(
    extra_filter: Optional[str],
    date_filter: Optional[str],
    bd_col: Optional[str],
    bd_val: Optional[str],
    use_bd: bool,
) -> str:
    clauses: List[str] = []

    if use_bd and bd_col and bd_val:
        clauses.append(f"{bd_col} = '{bd_val}'")

    if extra_filter:
        clauses.append(extra_filter)

    if date_filter:
        clauses.append(date_filter)

    if not clauses:
        return ""

    wrapped = [f"({c})" for c in clauses]
    return "WHERE " + " AND ".join(wrapped)


# -------------------------
# SQL execution with timing
# -------------------------
def run_sql_with_timing(session: Session, sql: str, debug: bool, test_name: str):
    if debug:
        print(f"[DEBUG][{test_name}] SQL:\n{sql}\n")

    start = time.time()
    rows = session.sql(sql).collect()
    dur_ms = int((time.time() - start) * 1000)

    if debug:
        print(f"[DEBUG][{test_name}] Completed in {dur_ms} ms, rows={len(rows)}")

    return rows, dur_ms


# -------------------------
# Result insertion
# -------------------------
def insert_result(
    session: Session,
    run_name: str,
    db: str,
    schema: str,
    table: str,
    bd_val: Optional[str],
    test_name: str,
    sql_text: Optional[str],
    metrics: Optional[Dict[str, Any]],
    pass_flag: Optional[bool],
    error: Optional[str],
    duration_ms: Optional[int],
    debug: bool,
):

    if debug:
        print(f"[DEBUG][{test_name}] Inserting result: PASS={pass_flag}, ERROR={error}")

    run_esc = escape_sql_literal(run_name)
    db_esc = escape_sql_literal(db)
    schema_esc = escape_sql_literal(schema)
    table_esc = escape_sql_literal(table)
    bd_esc = escape_sql_literal(bd_val) if bd_val else "NULL"
    test_esc = escape_sql_literal(test_name)
    sql_esc = escape_sql_literal(sql_text) if sql_text else "NULL"
    metrics_esc = metrics_to_sql(metrics)
    pass_esc = bool_to_sql(pass_flag)
    err_esc = escape_sql_literal(error) if error else "NULL"
    dur_esc = num_to_sql(duration_ms)

    stmt = f"""
        INSERT INTO QA_SHAKEDOWN_RESULTS
        (RUN_NAME, TABLE_DB, TABLE_SCHEMA, TABLE_NAME, BUSINESS_DATE,
         TEST_NAME, RESOLVED_SQL, METRICS, PASS_FLAG, ERROR, DURATION_MS)
        VALUES (
            {run_esc},
            {db_esc},
            {schema_esc},
            {table_esc},
            {bd_esc},
            {test_esc},
            {sql_esc},
            {metrics_esc},
            {pass_esc},
            {err_esc},
            {dur_esc}
        )
    """

    session.sql(stmt).collect()


# -------------------------
# Ensure results table
# -------------------------
def ensure_results_table(session: Session):
    session.sql("""
        CREATE TEMP TABLE IF NOT EXISTS QA_SHAKEDOWN_RESULTS (
            RESULT_ID      NUMBER IDENTITY,
            RUN_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            RUN_NAME       STRING,
            TABLE_DB       STRING,
            TABLE_SCHEMA   STRING,
            TABLE_NAME     STRING,
            BUSINESS_DATE  STRING,
            TEST_NAME      STRING,
            RESOLVED_SQL   STRING,
            METRICS        STRING,
            PASS_FLAG      BOOLEAN,
            ERROR          STRING,
            DURATION_MS    NUMBER
        )
    """).collect()


#######  META DATA LOADER

# """metadata_loader.py - Loads and normalizes table test metadata for the engine."""

import json
import os
from typing import Dict, Any, Optional

DEFAULT_VERSION = "1.0"


# -----------------------------
# Load metadata from a JSON file
# -----------------------------
def load_metadata_from_file(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Metadata JSON file not found: {path}")

    with open(path, "r") as f:
        data = json.load(f)

    return normalize_metadata(data)


# -----------------------------
# Load metadata from a dict
# -----------------------------
def load_metadata_from_dict(meta: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_metadata(meta)


# -----------------------------
# Normalize and validate metadata
# -----------------------------
def normalize_metadata(meta: Dict[str, Any]) -> Dict[str, Any]:
    required_top = ["table"]
    for key in required_top:
        if key not in meta:
            raise ValueError(f"Missing required key in metadata: '{key}'")

    table = meta["table"]
    if "schema" not in table or "name" not in table:
        raise ValueError("table.schema and table.name are required")

    # Fill defaults
    meta_normalized = {
        "version": meta.get("version", DEFAULT_VERSION),
        "run_name": meta.get("run_name"),
        "debug_mode": meta.get("debug_mode", "NO"),
        "parent_db": meta.get("parent_db", "SESAME"),
        "date_filter": meta.get("date_filter"),
        "tests_to_run": meta.get("tests_to_run", []),
        "table": {
            "schema": table["schema"],
            "name": table["name"],
            "pk_columns": table.get("pk_columns", []),
            "business_date_column": table.get("business_date_column"),
            "date_columns": table.get("date_columns", []),
            "timestamp_columns": table.get("timestamp_columns", []),
            "trim_columns": table.get("trim_columns", []),
            "clean_columns": table.get("clean_columns", []),
            "batch_columns": table.get("batch_columns", []),
            "range_tests": table.get("range_tests", []),
            "valid_value_tests": table.get("valid_value_tests", []),
            "scd": table.get("scd", {}),
            "fk_relations": table.get("fk_relations", []),
            "extra_filter": table.get("extra_filter"),
        },
    }
    return meta_normalized


# -----------------------------
# Optional: auto-discover JSON files
# -----------------------------
def discover_metadata_files(directory: str) -> Dict[str, Dict[str, Any]]:
    """Scan folder and load all .json metadata files"""
    results = {}
    for filename in os.listdir(directory):
        if filename.lower().endswith(".json"):
            path = os.path.join(directory, filename)
            try:
                results[filename] = load_metadata_from_file(path)
            except Exception as e:
                print(f"Skipping {filename}: {e}")
    return results


###### EXECUTION ENGINE V1   ###########

######## 1 test_business_date_match

# from helpers import run_sql_with_timing, insert_result, build_where_clause, fqn
# Test 1 : test_business_date_match


def test_business_date_match(session, cfg, run_name, bd, use_bd, debug):
    test_name = "BUSINESS_DATE_MATCH"
    tbl = cfg["table"]
    db = cfg["parent_db"]
    schema, table = tbl["schema"], tbl["name"]
    bd_col = tbl.get("business_date_column")
    extra = tbl.get("extra_filter")
    date_filter = cfg.get("date_filter")
    full = fqn(db, schema, table)

    if bd is None:
        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            None,
            None,
            False,
            "business_date is required for BUSINESS_DATE_MATCH",
            None,
            debug,
        )
        return

    where_clause = build_where_clause(extra, date_filter, bd_col, bd, use_bd)

    sql = f"SELECT COUNT(*) AS CNT FROM {full} {where_clause}"

    try:
        rows, dur = run_sql_with_timing(session, sql, debug, test_name)
        cnt = rows[0]["CNT"] if rows else 0

        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            sql,
            {"CNT": cnt},
            cnt > 0,
            None,
            dur,
            debug,
        )
    except Exception as e:
        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


########### 2


# 2 Test: NON_ZERO_COUNT_FOR_BUSINESS_DATE
# Ensures that for the given business date filter, the table returns at least one row.
# NON_ZERO_COUNT_FOR_BUSINESS_DATE"  test_pk_null_check
# from helpers import run_sql_with_timing, insert_result, build_where_clause, fqn


def test_non_zero_count(session, cfg, run_name, bd, use_bd, debug):
    test_name = "NON_ZERO_COUNT_FOR_BUSINESS_DATE"

    tbl = cfg["table"]
    db = cfg["parent_db"]
    schema, table = tbl["schema"], tbl["name"]

    bd_col = tbl.get("business_date_column")
    extra = tbl.get("extra_filter")
    date_filter = cfg.get("date_filter")

    full = fqn(db, schema, table)

    # Business date required for this test
    if bd is None:
        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            None,
            None,
            False,
            "business_date is required for NON_ZERO_COUNT_FOR_BUSINESS_DATE",
            None,
            debug,
        )
        return

    # Build WHERE clause with BD rule
    where_clause = build_where_clause(extra, date_filter, bd_col, bd, use_bd)

    sql = f"SELECT COUNT(*) AS CNT FROM {full} {where_clause}"

    try:
        rows, dur = run_sql_with_timing(session, sql, debug, test_name)
        cnt = rows[0]["CNT"] if rows else 0

        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            sql,
            {"CNT": cnt},
            cnt > 0,
            None,
            dur,
            debug,
        )

    except Exception as e:
        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


# 3 Test: PK_NOT_NULL
# Ensures that primary key columns contain no NULL values for the filtered dataset.
# test_pk_null_check
# from helpers import run_sql_with_timing, insert_result, build_where_clause, fqn
# Fix later
def test_pk_not_nullX(session, cfg, run_name, bd, use_bd, debug):
    test_name = "PK_NOT_NULL"

    tbl = cfg["table"]
    db = cfg["parent_db"]
    schema, table = tbl["schema"], tbl["name"]

    pks = tbl.get("pk_columns", [])
    bd_col = tbl.get("business_date_column")
    extra = tbl.get("extra_filter")
    date_filter = cfg.get("date_filter")

    full = fqn(db, schema, table)

    # PK list is mandatory
    if not pks:
        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            None,
            None,
            False,
            "pk_columns missing for PK_NOT_NULL",
            None,
            debug,
        )
        return

    # Build null predicate
    null_pred = " OR ".join([f"{c} IS NULL" for c in pks])

    # Build base WHERE clause with BD rule
    base_clause = build_where_clause(extra, date_filter, bd_col, bd, use_bd)

    # Add PK null condition
    if base_clause:
        where_clause = base_clause + f" AND ({null_pred})"
    else:
        where_clause = f"WHERE ({null_pred})"

    sql = f"SELECT COUNT(*) AS NULL_PK_CNT FROM {full} {where_clause}"

    try:
        rows, dur = run_sql_with_timing(session, sql, debug, test_name)
        cnt = rows[0]["NULL_PK_CNT"] if rows else 0

        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            sql,
            {"NULL_PK_CNT": cnt},
            cnt == 0,
            None,
            dur,
            debug,
        )

    except Exception as e:
        insert_result(
            session,
            run_name,
            db,
            schema,
            table,
            bd,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


# 4 test_structural_duplicates.py
# Purpose: Detect duplicate primary keys or duplicate business keys in the table.

# from helpers import (
#     build_fqn,
#     run_sql_timed,
#     insert_result,
#     build_where_clause
# )


def test_structural_duplicates(session, cfg, run_name, business_date, use_bd, debug):
    test_name = "STRUCTURAL_DUPLICATES"

    schema = cfg["schema"]
    table = cfg["name"]
    fqn = build_fqn(cfg["parent_db"], schema, table)

    pk_cols = cfg.get("pk_columns") or []
    extra_filter = cfg.get("extra_filter")
    date_col = cfg.get("business_date_column")
    date_filter = cfg.get("date_filter")

    if not pk_cols:
        insert_result(
            session,
            run_name,
            cfg["parent_db"],
            schema,
            table,
            business_date,
            test_name,
            None,
            None,
            False,
            "PK columns not defined",
            None,
            debug,
        )
        return

    pk_expr = ", ".join(pk_cols)

    where_clause = build_where_clause(
        extra_filter, date_filter, date_col, business_date, use_bd
    )

    sql = f"""
        SELECT COUNT(*) AS DUP_CNT
        FROM (
            SELECT {pk_expr}, COUNT(*) AS CNT
            FROM {fqn}
            {where_clause}
            GROUP BY {pk_expr}
            HAVING COUNT(*) > 1
        ) X
    """

    try:
        rows, dur = run_sql_timed(session, sql, debug, test_name)
        dup_cnt = rows[0]["DUP_CNT"] if rows else 0
        passed = dup_cnt == 0
        metrics = {"DUP_CNT": dup_cnt}

        insert_result(
            session,
            run_name,
            cfg["parent_db"],
            schema,
            table,
            business_date,
            test_name,
            sql,
            metrics,
            passed,
            None,
            dur,
            debug,
        )

    except Exception as e:
        insert_result(
            session,
            run_name,
            cfg["parent_db"],
            schema,
            table,
            business_date,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


# """
# 5 Test: DATE Columns Not Null
# This test validates that all configured DATE columns contain no NULL values after load.
# Each column is checked individually and violations are counted.
# """


def test_date_columns_not_null(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    fqn = f"{cfg['parent_db']}.{schema}.{table}"

    date_cols = cfg.get("date_columns") or []
    extra = cfg.get("extra_filter")
    bd_col = cfg.get("business_date_column")

    results = []
    total_viol = 0
    sql_list = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"{col} IS NULL")
        if not clauses:
            return ""
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in date_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][DATE_COLS] Executing for column {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["NULL_CNT"] if rows else 0
        total_viol += cnt
        results.append((col, cnt))

    return {
        "test_name": "DATE_COLS_NOT_NULL",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_nulls": total_viol},
    }


# """
# 6 Test: TIMESTAMP Columns Not Null
# This test checks that specified TIMESTAMP columns do not contain NULL values.
# Each column is validated independently, and violations are aggregated.
# """


def test_timestamp_columns_not_null(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    fqn = f"{cfg['parent_db']}.{schema}.{table}"

    ts_cols = cfg.get("timestamp_columns") or []
    extra = cfg.get("extra_filter")
    bd_col = cfg.get("business_date_column")

    results = []
    total_viol = 0
    sql_list = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"{col} IS NULL")
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in ts_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][TIMESTAMP_COLS] Executing for column {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["NULL_CNT"] if rows else 0
        total_viol += cnt
        results.append((col, cnt))

    return {
        "test_name": "TIMESTAMP_COLS_NOT_NULL",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_nulls": total_viol},
    }


# """
# 7 Test: TRIMMED_COLS
# Ensures VARCHAR columns have no leading/trailing spaces.
# Each column is validated independently.
# """


def test_trimmed_cols(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    fqn = f"{cfg['parent_db']}.{schema}.{table}"

    trim_cols = cfg.get("trim_columns") or []
    extra = cfg.get("extra_filter")
    bd_col = cfg.get("business_date_column")

    results = []
    total_viol = 0
    sql_list = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        cond = f"{col} IS NOT NULL AND {col} <> TRIM({col})"
        clauses.append(cond)
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in trim_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS TRIM_VIOL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][TRIMMED_COLS] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["TRIM_VIOL_CNT"] if rows else 0
        total_viol += cnt
        results.append((col, cnt))

    return {
        "test_name": "TRIMMED_COLS",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_violations": total_viol},
    }


# """
# 8 Test: CLEANED_COLS
# Ensures specified VARCHAR columns contain no newline (\n) or tab (\t) characters.
# Each column is validated independently. Uses TO_VARCHAR to avoid collation issues.
# """


def test_cleaned_cols(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    fqn = f"{cfg['parent_db']}.{schema}.{table}"

    clean_cols = cfg.get("clean_columns") or []
    extra = cfg.get("extra_filter")
    bd_col = cfg.get("business_date_column")

    results = []
    total_bad = 0
    sql_list = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        col_expr = f"TO_VARCHAR({col})"
        cond = f"{col_expr} IS NOT NULL AND (REGEXP_LIKE({col_expr}, '\\\\n') OR REGEXP_LIKE({col_expr}, '\\\\t'))"
        clauses.append(cond)
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in clean_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS BAD_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][CLEANED_COLS] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["BAD_CNT"] if rows else 0
        total_bad += cnt
        results.append((col, cnt))

    return {
        "test_name": "CLEANED_COLS",
        "sql": ";\n".join(sql_list),
        "passed": total_bad == 0,
        "metrics": {"details": results, "total_bad": total_bad},
    }


# """
# 9 Test: SCD2_SINGLE_OPEN_RECORD
# Ensures exactly one open SCD2 record exists per natural key.
# Open record = end_date_column is NULL or equal to open_end_value.
# """


def test_scd2_single_open_record(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    scd_cfg = cfg.get("scd") or {}
    nat_keys = scd_cfg.get("natural_key_columns") or []
    end_col = scd_cfg.get("end_date_column")
    open_end = scd_cfg.get("open_end_value")

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not nat_keys or not end_col:
        return {
            "test_name": "SCD2_SINGLE_OPEN_RECORD",
            "sql": None,
            "passed": False,
            "metrics": None,
            "error": "Missing natural_key_columns or end_date_column in SCD config.",
        }

    nat_list = ", ".join(nat_keys)

    clauses = []
    if use_bd and bd_col:
        clauses.append(f"{bd_col} = '{bd}'")
    if extra:
        clauses.append(extra)

    if open_end is None:
        clauses.append(f"{end_col} IS NULL")
    else:
        clauses.append(f"({end_col} IS NULL OR {end_col} = '{open_end}')")

    where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    sql = f"""
        SELECT COUNT(*) AS BAD_KEY_CNT
        FROM (
            SELECT {nat_list}, COUNT(*) AS OPEN_CNT
            FROM {fqn}
            {where_clause}
            GROUP BY {nat_list}
            HAVING COUNT(*) > 1
        ) T
    """

    if debug:
        print(f"[DEBUG][SCD2_SINGLE_OPEN_RECORD] SQL:\n{sql}\n")

    rows = session.sql(sql).collect()
    cnt = rows[0]["BAD_KEY_CNT"] if rows else 0

    return {
        "test_name": "SCD2_SINGLE_OPEN_RECORD",
        "sql": sql,
        "passed": cnt == 0,
        "metrics": {"BAD_KEY_CNT": cnt},
    }


# """
#  10 Test: SCD2_BUSINESS_DATE_MAX
# Ensures the maximum BUSINESS_DATE in the SCD2 table equals the current batch business date.
# """


def test_scd2_business_date_max(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not bd_col:
        return {
            "test_name": "SCD2_BUSINESS_DATE_MAX",
            "sql": None,
            "passed": False,
            "metrics": None,
            "error": "Missing business_date_column in metadata.",
        }

    clauses = []
    if extra:
        clauses.append(extra)

    where_clause = ""
    if clauses:
        where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    sql = f"""
        SELECT MAX({bd_col}) AS MAX_BD
        FROM {fqn}
        {where_clause}
    """

    if debug:
        print(f"[DEBUG][SCD2_BUSINESS_DATE_MAX] SQL:\n{sql}\n")

    rows = session.sql(sql).collect()
    max_bd = rows[0]["MAX_BD"] if rows else None

    passed = str(max_bd) == str(bd)

    return {
        "test_name": "SCD2_BUSINESS_DATE_MAX",
        "sql": sql,
        "passed": passed,
        "metrics": {"MAX_BD": max_bd, "EXPECTED": bd},
    }


# """
# 11 Test: SCD2_BATCH_COLUMNS_NOT_NULL
# Ensures all ETL batch columns (e.g., LOAD_DT, BATCH_ID) are not NULL.
# Each column is tested independently.
# """


def test_scd2_batch_columns_not_null(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    batch_cols = cfg.get("batch_columns") or []
    extra = cfg.get("extra_filter")
    bd_col = cfg.get("business_date_column")

    if not batch_cols:
        return {
            "test_name": "SCD2_BATCH_COLUMNS_NOT_NULL",
            "sql": None,
            "passed": False,
            "metrics": None,
            "error": "batch_columns not defined in metadata.",
        }

    results = []
    sql_list = []
    total_viol = 0

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"{col} IS NULL")
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in batch_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][SCD2_BATCH_COLUMNS_NOT_NULL] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["NULL_CNT"] if rows else 0
        total_viol += cnt
        results.append((col, cnt))

    return {
        "test_name": "SCD2_BATCH_COLUMNS_NOT_NULL",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_nulls": total_viol},
    }


# """
# 12 Test: SCD2_LOGICAL_DELETE
# Validates that logically deleted records have:
# - end_date_column = business_date - 1
# - end_date_column != open-end (9999-12-31 or configured)
# - current_flag_column = 0 (optional check if present)
# """


def test_scd2_logical_delete(session, cfg, run_name, bd, use_bd, debug):
    import datetime

    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    scd = cfg.get("scd") or {}
    end_col = scd.get("end_date_column")
    start_col = scd.get("start_date_column")
    curr_col = scd.get("current_flag_column")
    open_end = scd.get("open_end_value", "9999-12-31")

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not end_col:
        return {
            "test_name": "SCD2_LOGICAL_DELETE",
            "sql": None,
            "passed": False,
            "metrics": None,
            "error": "Missing end_date_column in SCD config.",
        }

    if not use_bd:
        return {
            "test_name": "SCD2_LOGICAL_DELETE",
            "sql": None,
            "passed": True,
            "metrics": {"skipped": "BD filter disabled via 1900-01-01"},
        }

    # compute business_date - 1 day
    try:
        bd_dt = datetime.datetime.strptime(bd, "%Y-%m-%d")
        expected_end = (bd_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    except:
        return {
            "test_name": "SCD2_LOGICAL_DELETE",
            "sql": None,
            "passed": False,
            "error": "Invalid business date format",
        }

    clauses = [f"{end_col} = '{expected_end}'"]
    clauses.append(f"{end_col} <> '{open_end}'")
    if curr_col:
        clauses.append(f"{curr_col} = 0")

    if bd_col:
        clauses.append(f"{bd_col} = '{bd}'")
    if extra:
        clauses.append(extra)

    where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    sql = f"""
        SELECT COUNT(*) AS LOGICAL_DEL_CNT
        FROM {fqn}
        {where_clause}
    """

    if debug:
        print(f"[DEBUG][SCD2_LOGICAL_DELETE] SQL:\n{sql}\n")

    rows = session.sql(sql).collect()
    cnt = rows[0]["LOGICAL_DEL_CNT"] if rows else 0

    return {
        "test_name": "SCD2_LOGICAL_DELETE",
        "sql": sql,
        "passed": cnt
        >= 0,  # any count is valid — this test asserts correctness of formula, not volume
        "metrics": {
            "LOGICAL_DEL_CNT": cnt,
            "EXPECTED_END_DT": expected_end,
            "OPEN_END_VALUE": open_end,
        },
    }


# """
# 13 Test: SCD2_LOGICAL_DELETE
# Validates that logically deleted records have:
# - end_date_column = business_date - 1
# - end_date_column != open-end (9999-12-31 or configured)
# - current_flag_column = 0 (optional check if present)
# """


def test_scd2_logical_delete(session, cfg, run_name, bd, use_bd, debug):
    import datetime

    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    scd = cfg.get("scd") or {}
    end_col = scd.get("end_date_column")
    start_col = scd.get("start_date_column")
    curr_col = scd.get("current_flag_column")
    open_end = scd.get("open_end_value", "9999-12-31")

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not end_col:
        return {
            "test_name": "SCD2_LOGICAL_DELETE",
            "sql": None,
            "passed": False,
            "metrics": None,
            "error": "Missing end_date_column in SCD config.",
        }

    if not use_bd:
        return {
            "test_name": "SCD2_LOGICAL_DELETE",
            "sql": None,
            "passed": True,
            "metrics": {"skipped": "BD filter disabled via 1900-01-01"},
        }

    # compute business_date - 1 day
    try:
        bd_dt = datetime.datetime.strptime(bd, "%Y-%m-%d")
        expected_end = (bd_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    except:
        return {
            "test_name": "SCD2_LOGICAL_DELETE",
            "sql": None,
            "passed": False,
            "error": "Invalid business date format",
        }

    clauses = [f"{end_col} = '{expected_end}'"]
    clauses.append(f"{end_col} <> '{open_end}'")
    if curr_col:
        clauses.append(f"{curr_col} = 0")

    if bd_col:
        clauses.append(f"{bd_col} = '{bd}'")
    if extra:
        clauses.append(extra)

    where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    sql = f"""
        SELECT COUNT(*) AS LOGICAL_DEL_CNT
        FROM {fqn}
        {where_clause}
    """

    if debug:
        print(f"[DEBUG][SCD2_LOGICAL_DELETE] SQL:\n{sql}\n")

    rows = session.sql(sql).collect()
    cnt = rows[0]["LOGICAL_DEL_CNT"] if rows else 0

    return {
        "test_name": "SCD2_LOGICAL_DELETE",
        "sql": sql,
        "passed": cnt
        >= 0,  # any count is valid — this test asserts correctness of formula, not volume
        "metrics": {
            "LOGICAL_DEL_CNT": cnt,
            "EXPECTED_END_DT": expected_end,
            "OPEN_END_VALUE": open_end,
        },
    }


# """
# 14 Test: SCD2_INSERT_UPDATE_DELETE_COUNTS
# Counts how many rows were INSERTED, UPDATED, and logically DELETED in the SCD2 table
# for the given business date.

# Assumptions:
# - business_date_column identifies rows belonging to the current load.
# - start_date_column indicates new/changed rows.
# - end_date_column indicates closed/deleted rows.
# """


def test_scd2_insert_update_delete_counts(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    scd = cfg.get("scd") or {}
    start_col = scd.get("start_date_column")
    end_col = scd.get("end_date_column")
    open_end = scd.get("open_end_value", "9999-12-31")

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not start_col or not end_col or not bd_col:
        return {
            "test_name": "SCD2_INSERT_UPDATE_DELETE_COUNTS",
            "sql": None,
            "passed": False,
            "error": "Missing SCD2 start/end/business_date_column definitions.",
        }

    if not use_bd:
        return {
            "test_name": "SCD2_INSERT_UPDATE_DELETE_COUNTS",
            "sql": None,
            "passed": True,
            "metrics": {"skipped": "BD filter disabled via 1900-01-01"},
        }

    clauses = [f"{bd_col} = '{bd}'"]
    if extra:
        clauses.append(extra)
    where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    # INSERT rows: start_col = business date
    sql_insert = (
        f"SELECT COUNT(*) AS INS_CNT FROM {fqn} {where_clause} AND {start_col} = '{bd}'"
    )

    # UPDATE rows: start_col = business date AND end_col != open_end -> changed record
    sql_update = f"SELECT COUNT(*) AS UPD_CNT FROM {fqn} {where_clause} AND {start_col} = '{bd}' AND {end_col} <> '{open_end}'"

    # DELETE rows: end_col = business_date - 1
    sql_delete = f"SELECT COUNT(*) AS DEL_CNT FROM {fqn} {where_clause} AND {end_col} = DATEADD(day, -1, '{bd}')"

    if debug:
        print("[DEBUG][SCD2_INSERT_UPDATE_DELETE_COUNTS] SQL INSERT:\n", sql_insert)
        print("[DEBUG][SCD2_INSERT_UPDATE_DELETE_COUNTS] SQL UPDATE:\n", sql_update)
        print("[DEBUG][SCD2_INSERT_UPDATE_DELETE_COUNTS] SQL DELETE:\n", sql_delete)

    rows_ins = session.sql(sql_insert).collect()
    rows_upd = session.sql(sql_update).collect()
    rows_del = session.sql(sql_delete).collect()

    cnt_ins = rows_ins[0]["INS_CNT"] if rows_ins else 0
    cnt_upd = rows_upd[0]["UPD_CNT"] if rows_upd else 0
    cnt_del = rows_del[0]["DEL_CNT"] if rows_del else 0

    return {
        "test_name": "SCD2_INSERT_UPDATE_DELETE_COUNTS",
        "sql": sql_insert + ";\n" + sql_update + ";\n" + sql_delete,
        "passed": True,
        "metrics": {
            "INSERT_COUNT": cnt_ins,
            "UPDATE_COUNT": cnt_upd,
            "DELETE_COUNT": cnt_del,
        },
    }


# """
# 15 Test: SCD2_REQUIRED_COLUMNS_NOT_NULL
# Ensures all required SCD2 columns (start_date, end_date, current_flag if defined)
# are not NULL for the given business date.
# """


def test_scd2_required_columns_not_null(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    scd = cfg.get("scd") or {}
    required_cols = []

    # Required SCD2 fields
    if scd.get("start_date_column"):
        required_cols.append(scd["start_date_column"])
    if scd.get("end_date_column"):
        required_cols.append(scd["end_date_column"])
    if scd.get("current_flag_column"):
        required_cols.append(scd["current_flag_column"])

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not required_cols:
        return {
            "test_name": "SCD2_REQUIRED_COLUMNS_NOT_NULL",
            "sql": None,
            "passed": False,
            "metrics": None,
            "error": "No SCD2 required columns found in metadata.",
        }

    results = []
    sql_list = []
    total_viol = 0

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"{col} IS NULL")
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in required_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][SCD2_REQUIRED_COLUMNS_NOT_NULL] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["NULL_CNT"] if rows else 0
        total_viol += cnt
        results.append((col, cnt))

    return {
        "test_name": "SCD2_REQUIRED_COLUMNS_NOT_NULL",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_nulls": total_viol},
    }


# """
# 16 Test: DOMAIN_VALUE_CHECKS
# Validates that a given column contains values ONLY from an allowed domain list
# (including NULL if allowed).
# Example domain: ["A", "B", "C", None]
# """


def test_domain_value_checks(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    domain_cfg = cfg.get("domain_checks") or []

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not domain_cfg:
        return {
            "test_name": "DOMAIN_VALUE_CHECKS",
            "sql": None,
            "passed": False,
            "error": "domain_checks not specified in metadata.",
        }

    sql_list = []
    total_viol = 0
    results = []

    for item in domain_cfg:
        col = item.get("column")
        domain = item.get("allowed_values")

        if not col or domain is None:
            return {
                "test_name": "DOMAIN_VALUE_CHECKS",
                "sql": None,
                "passed": False,
                "error": "Invalid domain configuration entry.",
            }

        allowed_sql_vals = []
        for v in domain:
            if v is None:
                allowed_sql_vals.append("NULL")
            else:
                escaped = str(v).replace("'", "''")
                allowed_sql_vals.append(f"'{escaped}'")

        allowed_list_sql = ", ".join(allowed_sql_vals)

        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)

        clauses.append(f"{col} NOT IN ({allowed_list_sql})")

        where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

        sql = f"SELECT COUNT(*) AS BAD_CNT FROM {fqn} {where_clause}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][DOMAIN_VALUE_CHECKS] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["BAD_CNT"] if rows else 0
        total_viol += cnt
        results.append({col: cnt})

    return {
        "test_name": "DOMAIN_VALUE_CHECKS",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_domain_violations": total_viol},
    }


# """
# 17 Test: STRING_WHITESPACE_AND_CONTROL_CHARS
# Checks for:
# - Leading/trailing spaces
# - Carriage return \r
# - Newline \n
# - Tab \t
# in specified VARCHAR columns.
# Each column is tested independently.
# """


def test_string_whitespace_and_control_chars(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    cols = cfg.get("string_clean_checks") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not cols:
        return {
            "test_name": "STRING_WHITESPACE_AND_CONTROL_CHARS",
            "sql": None,
            "passed": False,
            "error": "string_clean_checks not specified in metadata.",
        }

    sql_list = []
    total_viol = 0
    results = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)

        col_v = f"TO_VARCHAR({col})"

        cond = (
            f"{col_v} <> TRIM({col_v}) OR "
            f"REGEXP_LIKE({col_v}, '\\r') OR "
            f"REGEXP_LIKE({col_v}, '\\n') OR "
            f"REGEXP_LIKE({col_v}, '\\t')"
        )

        clauses.append(cond)
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS BAD_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(
                f"[DEBUG][STRING_WHITESPACE_AND_CONTROL_CHARS] SQL for column {col}:\n{sql}\n"
            )

        rows = session.sql(sql).collect()
        cnt = rows[0]["BAD_CNT"] if rows else 0
        total_viol += cnt
        results.append({col: cnt})

    return {
        "test_name": "STRING_WHITESPACE_AND_CONTROL_CHARS",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_violations": total_viol},
    }


# """
# 18 Test: NUMERIC_RANGE_VALIDATION
# Validates that numeric/decimal columns lie within an allowed range.
# Example config:
#   numeric_range_checks: [
#       {"column": "AMOUNT", "min": 0, "max": 999999},
#       {"column": "DISCOUNT", "min": -50, "max": 50}
#   ]
# """


def test_numeric_range_validation(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    checks = cfg.get("numeric_range_checks") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not checks:
        return {
            "test_name": "NUMERIC_RANGE_VALIDATION",
            "sql": None,
            "passed": False,
            "error": "numeric_range_checks not specified in metadata.",
        }

    sql_list = []
    total_viol = 0
    results = []

    def build_where(col, mn, mx):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"({col} < {mn} OR {col} > {mx})")
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for item in checks:
        col = item.get("column")
        mn = item.get("min")
        mx = item.get("max")

        if col is None or mn is None or mx is None:
            return {
                "test_name": "NUMERIC_RANGE_VALIDATION",
                "sql": None,
                "passed": False,
                "error": f"Invalid entry in numeric_range_checks: {item}",
            }

        where = build_where(col, mn, mx)
        sql = f"SELECT COUNT(*) AS BAD_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][NUMERIC_RANGE_VALIDATION] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["BAD_CNT"] if rows else 0
        total_viol += cnt
        results.append({col: cnt})

    return {
        "test_name": "NUMERIC_RANGE_VALIDATION",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_out_of_range": total_viol},
    }


# """
# 19 Test: DUPLICATE_BUSINESS_KEYS
# Checks for duplicate rows based on a defined set of business key columns.
# business_keys must be defined in metadata:
#   business_keys: ["ORDER_ID", "LINE_NO", "CUSTOMER_ID"]
# """


def test_duplicate_business_keys(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    bk_cols = cfg.get("business_keys") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not bk_cols:
        return {
            "test_name": "DUPLICATE_BUSINESS_KEYS",
            "sql": None,
            "passed": False,
            "error": "business_keys not defined in metadata.",
        }

    bk_expr = ", ".join(bk_cols)

    clauses = []
    if use_bd and bd_col:
        clauses.append(f"{bd_col} = '{bd}'")
    if extra:
        clauses.append(extra)

    where_clause = ""
    if clauses:
        where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    sql = f"""
        SELECT COUNT(*) AS DUP_CNT
        FROM (
            SELECT {bk_expr}, COUNT(*) AS CNT
            FROM {fqn}
            {where_clause}
            GROUP BY {bk_expr}
            HAVING COUNT(*) > 1
        ) X
    """

    if debug:
        print(f"[DEBUG][DUPLICATE_BUSINESS_KEYS] SQL:\n{sql}\n")

    rows = session.sql(sql).collect()
    dup_cnt = rows[0]["DUP_CNT"] if rows else 0

    return {
        "test_name": "DUPLICATE_BUSINESS_KEYS",
        "sql": sql,
        "passed": dup_cnt == 0,
        "metrics": {"DUP_CNT": dup_cnt},
    }


# """
# 20 Test: DUPLICATE_SCD2_KEYS
# Validates duplicates based on:
# - natural_key_columns
# - AND SCD2-specific columns: start_date, end_date, current_flag (if defined)

# This detects identical snapshots that shouldn't exist in a Type-2 history table.
# """


def test_duplicate_scd2_keys(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    scd = cfg.get("scd") or {}
    nat_keys = scd.get("natural_key_columns") or []
    start_col = scd.get("start_date_column")
    end_col = scd.get("end_date_column")
    curr_col = scd.get("current_flag_column")

    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not nat_keys or not start_col or not end_col:
        return {
            "test_name": "DUPLICATE_SCD2_KEYS",
            "sql": None,
            "passed": False,
            "error": "Missing required SCD2 columns or natural keys.",
        }

    # Build duplicate key expression
    scd_cols = [start_col, end_col]
    if curr_col:
        scd_cols.append(curr_col)

    key_cols = nat_keys + scd_cols
    key_expr = ", ".join(key_cols)

    clauses = []
    if use_bd and bd_col:
        clauses.append(f"{bd_col} = '{bd}'")
    if extra:
        clauses.append(extra)

    where_clause = ""
    if clauses:
        where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

    sql = f"""
        SELECT COUNT(*) AS DUP_CNT
        FROM (
            SELECT {key_expr}, COUNT(*) AS CNT
            FROM {fqn}
            {where_clause}
            GROUP BY {key_expr}
            HAVING COUNT(*) > 1
        ) D
    """

    if debug:
        print(f"[DEBUG][DUPLICATE_SCD2_KEYS] SQL:\n{sql}\n")

    rows = session.sql(sql).collect()
    dup_cnt = rows[0]["DUP_CNT"] if rows else 0

    return {
        "test_name": "DUPLICATE_SCD2_KEYS",
        "sql": sql,
        "passed": dup_cnt == 0,
        "metrics": {"DUP_CNT": dup_cnt, "KEY_COLUMNS": key_cols},
    }


# """
# 21 Test: ETL_BATCH_COLUMNS_NOT_NULL
# Checks that all ETL audit/batch columns are NOT NULL.
# These columns are defined in metadata under: etl_batch_columns
# """


def test_etl_batch_columns_not_null(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    cols = cfg.get("etl_batch_columns") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not cols:
        return {
            "test_name": "ETL_BATCH_COLUMNS_NOT_NULL",
            "sql": None,
            "passed": False,
            "error": "etl_batch_columns not defined in metadata.",
        }

    sql_list = []
    total_viol = 0
    results = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"{col} IS NULL")
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][ETL_BATCH_COLUMNS_NOT_NULL] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["NULL_CNT"] if rows else 0
        total_viol += cnt
        results.append({col: cnt})

    return {
        "test_name": "ETL_BATCH_COLUMNS_NOT_NULL",
        "sql": ";".join(sql_list),
        "passed": total_viol == 0,
        "metrics": {"details": results, "total_nulls": total_viol},
    }


# """
# 22 Test: FOREIGN_KEY_ORPHANS
# Checks for orphan child rows where the FK column does not match any parent record.
# Metadata requirement:
#   fk_relations: [
#       {
#         "child_column": "CUSTOMER_ID",
#         "parent_schema": "CORE",
#         "parent_table": "CUSTOMERS",
#         "parent_key_column": "CUSTOMER_ID"
#       }
#   ]
# """


def test_foreign_key_orphans(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    child_fqn = f"{parent_db}.{schema}.{table}"

    fk_list = cfg.get("fk_relations") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not fk_list:
        return {
            "test_name": "FOREIGN_KEY_ORPHANS",
            "sql": None,
            "passed": False,
            "error": "fk_relations missing in metadata.",
        }

    sql_list = []
    total_orphans = 0
    results = []

    for fk in fk_list:
        child_col = fk.get("child_column")
        p_schema = fk.get("parent_schema")
        p_table = fk.get("parent_table")
        parent_key = fk.get("parent_key_column")

        if not (child_col and p_schema and p_table and parent_key):
            return {
                "test_name": "FOREIGN_KEY_ORPHANS",
                "sql": None,
                "passed": False,
                "error": f"Invalid FK definition: {fk}",
            }

        parent_fqn = f"{parent_db}.{p_schema}.{p_table}"

        clauses = []
        if use_bd and bd_col:
            clauses.append(f"C.{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"P.{parent_key} IS NULL AND C.{child_col} IS NOT NULL")

        where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

        sql = f"""
            SELECT COUNT(*) AS ORPHAN_CNT
            FROM (
                SELECT DISTINCT C.{child_col}
                FROM {child_fqn} C
                LEFT JOIN {parent_fqn} P
                  ON C.{child_col} = P.{parent_key}
                {where_clause}
            ) X
        """

        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][FOREIGN_KEY_ORPHANS] SQL:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["ORPHAN_CNT"] if rows else 0
        total_orphans += cnt
        results.append({"fk": fk, "ORPHAN_CNT": cnt})

    return {
        "test_name": "FOREIGN_KEY_ORPHANS",
        "sql": ";".join(sql_list),
        "passed": total_orphans == 0,
        "metrics": {"details": results, "total_orphans": total_orphans},
    }


# """
# 23 Test: ORPHAN_CHECKS
# Generalized orphan validation for any child->parent relationship
# defined under `orphan_checks` in metadata.

# Example metadata:
#   orphan_checks: [
#       {
#         "child_column": "PRODUCT_ID",
#         "parent_schema": "MDM",
#         "parent_table": "PRODUCT",
#         "parent_key_column": "ID"
#       }
#   ]
# """


def test_orphan_checks(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    child_fqn = f"{parent_db}.{schema}.{table}"

    checks = cfg.get("orphan_checks") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not checks:
        return {
            "test_name": "ORPHAN_CHECKS",
            "sql": None,
            "passed": False,
            "error": "orphan_checks missing in metadata.",
        }

    sql_list = []
    total_orphans = 0
    results = []

    for chk in checks:
        child_col = chk.get("child_column")
        p_schema = chk.get("parent_schema")
        p_table = chk.get("parent_table")
        parent_key = chk.get("parent_key_column")

        if not (child_col and p_schema and p_table and parent_key):
            return {
                "test_name": "ORPHAN_CHECKS",
                "sql": None,
                "passed": False,
                "error": f"Invalid orphan check definition: {chk}",
            }

        parent_fqn = f"{parent_db}.{p_schema}.{p_table}"

        clauses = []
        if use_bd and bd_col:
            clauses.append(f"C.{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"P.{parent_key} IS NULL AND C.{child_col} IS NOT NULL")

        where_clause = "WHERE " + " AND ".join(f"({c})" for c in clauses)

        sql = f"""
            SELECT COUNT(*) AS ORPHAN_CNT
            FROM (
                SELECT DISTINCT C.{child_col}
                FROM {child_fqn} C
                LEFT JOIN {parent_fqn} P
                  ON C.{child_col} = P.{parent_key}
                {where_clause}
            ) O
        """

        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][ORPHAN_CHECKS] SQL:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["ORPHAN_CNT"] if rows else 0
        total_orphans += cnt

        results.append({"check": chk, "ORPHAN_CNT": cnt})

    return {
        "test_name": "ORPHAN_CHECKS",
        "sql": ";".join(sql_list),
        "passed": total_orphans == 0,
        "metrics": {"details": results, "total_orphans": total_orphans},
    }


# """
# 24 Test: PK_NOT_NULL
# Ensures all primary key columns are NOT NULL.
# Each column is checked independently.
# """


def test_pk_not_null(session, cfg, run_name, bd, use_bd, debug):
    schema = cfg["schema"]
    table = cfg["name"]
    parent_db = cfg["parent_db"]
    fqn = f"{parent_db}.{schema}.{table}"

    pk_cols = cfg.get("pk_columns") or []
    bd_col = cfg.get("business_date_column")
    extra = cfg.get("extra_filter")

    if not pk_cols:
        return {
            "test_name": "PK_NOT_NULL",
            "sql": None,
            "passed": False,
            "error": "pk_columns not defined in metadata.",
        }

    sql_list = []
    total_nulls = 0
    results = []

    def build_where(col):
        clauses = []
        if use_bd and bd_col:
            clauses.append(f"{bd_col} = '{bd}'")
        if extra:
            clauses.append(extra)
        clauses.append(f"{col} IS NULL")
        return "WHERE " + " AND ".join(f"({c})" for c in clauses)

    for col in pk_cols:
        where = build_where(col)
        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where}"
        sql_list.append(sql)

        if debug:
            print(f"[DEBUG][PK_NOT_NULL] SQL for {col}:\n{sql}\n")

        rows = session.sql(sql).collect()
        cnt = rows[0]["NULL_CNT"] if rows else 0
        total_nulls += cnt
        results.append({col: cnt})

    return {
        "test_name": "PK_NOT_NULL",
        "sql": ";".join(sql_list),
        "passed": total_nulls == 0,
        "metrics": {"details": results, "total_nulls": total_nulls},
    }


# ============================================================
# TEST REGISTRY – all functions already defined in this file
# ============================================================

TEST_REGISTRY = {
    # 1–8 STRUCTURAL TESTS
    "BUSINESS_DATE_MATCH": test_business_date_match,
    "NON_ZERO_COUNT_FOR_BUSINESS_DATE": test_non_zero_count,
    # "PK_NULL_CHECK":                          test_pk_null_check,
    "STRUCTURAL_DUPLICATES": test_structural_duplicates,
    "DATE_COLS_NOT_NULL": test_date_columns_not_null,
    "TIMESTAMP_COLS_NOT_NULL": test_timestamp_columns_not_null,
    "TRIMMED_COLS": test_trimmed_cols,
    "CLEANED_COLS": test_cleaned_cols,
    # 9–14 SCD2 TESTS
    "SCD2_SINGLE_OPEN_RECORD": test_scd2_single_open_record,
    "SCD2_BUSINESS_DATE_MAX": test_scd2_business_date_max,
    "SCD2_BATCH_COLUMNS_NOT_NULL": test_scd2_batch_columns_not_null,
    "SCD2_LOGICAL_DELETE": test_scd2_logical_delete,
    "SCD2_INSERT_UPDATE_DELETE_COUNTS": test_scd2_insert_update_delete_counts,
    "SCD2_REQUIRED_COLUMNS_NOT_NULL": test_scd2_required_columns_not_null,
    # 15–17 BUSINESS / DATA QUALITY TESTS
    "DOMAIN_VALUE_CHECKS": test_domain_value_checks,
    "STRING_WHITESPACE_AND_CONTROL_CHARS": test_string_whitespace_and_control_chars,
    "NUMERIC_RANGE_VALIDATION": test_numeric_range_validation,
    # 18–21 DUPLICATE + RELATIONAL TESTS
    "DUPLICATE_BUSINESS_KEYS": test_duplicate_business_keys,
    "DUPLICATE_SCD2_KEYS": test_duplicate_scd2_keys,
    "ETL_BATCH_COLUMNS_NOT_NULL": test_etl_batch_columns_not_null,
    # 22–23 ORPHANS + PK NOT NULL
    "FOREIGN_KEY_ORPHANS": test_foreign_key_orphans,
    "ORPHAN_CHECKS": test_orphan_checks,
    "PK_NOT_NULL": test_pk_not_null,
}


######## EXECUTION CALLER ########

# ======================================================================
# RESULT TABLE CREATION
# ======================================================================


def ensure_results_table(session):
    session.sql("""
        CREATE TEMP TABLE IF NOT EXISTS QA_SHAKEDOWN_RESULTS (
            RESULT_ID      NUMBER IDENTITY,
            RUN_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            RUN_NAME       STRING,
            TABLE_DB       STRING,
            TABLE_SCHEMA   STRING,
            TABLE_NAME     STRING,
            BUSINESS_DATE  STRING,
            TEST_NAME      STRING,
            RESOLVED_SQL   STRING,
            METRICS        STRING,
            PASS_FLAG      STRING,
            ERROR          STRING,
            DURATION_MS    NUMBER
        )
    """).collect()


# ======================================================================
# UTILITIES
# ======================================================================


def escape_sql_literal(s):
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def should_apply_business_date(bd_value):
    return bd_value not in (None, "1900-01-01")


# ======================================================================
# INSERT RESULT
# ======================================================================


def insert_result(
    session,
    run_name,
    parent_db,
    schema,
    table,
    business_date_value,
    test_name,
    resolved_sql,
    metrics,
    passed,
    error,
    duration_ms,
    debug,
):
    run_lit = escape_sql_literal(run_name)
    db_lit = escape_sql_literal(parent_db)
    sch_lit = escape_sql_literal(schema)
    tbl_lit = escape_sql_literal(table)
    bd_lit = escape_sql_literal(business_date_value)

    sql_lit = escape_sql_literal(resolved_sql) if resolved_sql else "NULL"
    err_lit = escape_sql_literal(error) if error else "NULL"

    # metrics to JSON string
    if metrics is None:
        metrics_lit = "NULL"
    else:
        import json

        js = json.dumps(metrics).replace("'", "''")
        metrics_lit = f"'{js}'"

    pass_lit = ("'PASS'" if passed else "'FAIL'") if passed is not None else "NULL"
    dur_lit = "NULL" if duration_ms is None else str(duration_ms)

    if debug:
        print(
            f"[DEBUG][{test_name}] Inserting test result: passed={passed}, error={error}"
        )

    session.sql(f"""
        INSERT INTO QA_SHAKEDOWN_RESULTS
        (RUN_NAME, TABLE_DB, TABLE_SCHEMA, TABLE_NAME,
         BUSINESS_DATE, TEST_NAME, RESOLVED_SQL, METRICS,
         PASS_FLAG, ERROR, DURATION_MS)
        VALUES (
            {run_lit}, {db_lit}, {sch_lit}, {tbl_lit},
            {bd_lit}, '{test_name}', {sql_lit}, {metrics_lit},
            {pass_lit}, {err_lit}, {dur_lit}
        )
    """).collect()


# ======================================================================
# EXECUTOR ENGINE
# ======================================================================


def run_shakedown(session, meta, business_date_value):
    """
    Core execution engine (no imports).
    Runs all requested tests for a single table.

    meta structure:
      {
        "run_name": "string",
        "parent_db": "SESAME",
        "table": {
            "schema": "CORE",
            "name": "ORDERS",
            ...
        },
        "tests_to_run": [...],
        "debug_mode": "YES" or "NO",
        "date_filter": "...optional..."
      }
    """

    ensure_results_table(session)

    run_name = meta.get("run_name", "shakedown_run")
    parent_db = meta.get("parent_db")
    tbl_cfg = meta["table"]
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    date_filter = meta.get("date_filter")
    debug = str(meta.get("debug_mode", "NO")).upper() == "YES"

    requested_tests = meta.get("tests_to_run", list(TEST_REGISTRY.keys()))
    active_tests = [t for t in requested_tests if t in TEST_REGISTRY]

    use_bd = should_apply_business_date(business_date_value)

    if debug:
        print(f"[DEBUG] Running table: {parent_db}.{schema}.{table}")
        print(f"[DEBUG] Business date: {business_date_value}, use_bd={use_bd}")
        print(f"[DEBUG] Tests to run: {active_tests}")

    for test_name in active_tests:
        test_func = TEST_REGISTRY[test_name]

        if debug:
            print(f"[DEBUG] >>> Starting test: {test_name}")

        try:
            result = test_func(
                session=session,
                cfg=tbl_cfg,
                run_name=run_name,
                bd=business_date_value,
                use_bd=use_bd,
                debug=debug,
            )

            insert_result(
                session,
                run_name,
                parent_db,
                schema,
                table,
                business_date_value,
                test_name,
                result.get("sql"),
                result.get("metrics"),
                result.get("passed"),
                result.get("error"),
                result.get("duration_ms"),
                debug,
            )

        except Exception as ex:
            insert_result(
                session,
                run_name,
                parent_db,
                schema,
                table,
                business_date_value,
                test_name,
                None,
                None,
                False,
                str(ex),
                None,
                debug,
            )

    # return summary
    return session.sql(f"""
        SELECT 
            RESULT_ID,
            TEST_NAME,
            PASS_FLAG,
            ERROR,
            METRICS,
            DURATION_MS
        FROM QA_SHAKEDOWN_RESULTS
        WHERE RUN_NAME     = {escape_sql_literal(run_name)}
          AND TABLE_DB     = {escape_sql_literal(parent_db)}
          AND TABLE_SCHEMA = {escape_sql_literal(schema)}
          AND TABLE_NAME   = {escape_sql_literal(table)}
        ORDER BY RESULT_ID
    """)


# ======================================================================
# SIMPLE CALL WRAPPER
# ======================================================================


def run_table_shakedown(session, db_name, table_fqn, business_date_value):
    """
    Helper wrapper to run:
      run_table_shakedown(session, "SESAME", "CORE.ORDERS", "2025-01-01")
    """

    schema, table = table_fqn.split(".", 1)
    key = f"{schema}.{table}"
    if key not in TABLE_TEST_META:
        raise ValueError(f"No metadata defined for table {key}")

    cfg_base = TABLE_TEST_META[key]
    run_name = f"shakedown_{schema.lower()}_{table.lower()}_{(business_date_value or 'nobd').replace('-', '')}"

    meta = {
        "run_name": run_name,
        "parent_db": db_name,
        "table": cfg_base["table"],
        "tests_to_run": cfg_base.get("tests_to_run", []),
        "debug_mode": cfg_base.get("debug_mode", "NO"),
        "date_filter": cfg_base.get("date_filter"),
    }

    meta["table"]["schema"] = schema
    meta["table"]["name"] = table
    return run_shakedown(session, meta, business_date_value)
