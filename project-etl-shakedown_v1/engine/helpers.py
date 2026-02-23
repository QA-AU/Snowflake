# =========================================================
# engine/helpers.py
# Shared utilities used by all test modules
# =========================================================

import json
import time
from typing import Dict


# ---------------------------------------------------------
# Run SQL with timing
# ---------------------------------------------------------
def run_sql_with_timing(session, sql: str):
    """
    Executes SQL and measures execution time in ms.
    Returns (rows: list, duration_ms: int)
    """
    start = time.time()
    rows = session.sql(sql).collect()
    duration_ms = int((time.time() - start) * 1000)
    return rows, duration_ms


# ---------------------------------------------------------
# Insert a test result row into STG.QA_SHAKEDOWN_RESULTS
# ---------------------------------------------------------
def insert_result_row(
    session,
    meta: Dict,
    run_name: str,
    test_name: str,
    sql_used: str,
    passed: bool,
    metrics: Dict,
    error: str,
    duration_ms: int,
):
    """
    Inserts a single test result.
    Uses $$ quoting for safe SQL.
    """

    metrics_json = json.dumps(metrics) if metrics else None
    error_json = error if error else None

    session.sql(f"""
        INSERT INTO STG.QA_SHAKEDOWN_RESULTS (
            RUN_NAME,
            TABLE_FQN,
            TEST_NAME,
            SQL_USED,
            PASS_FLAG,
            METRICS,
            ERROR,
            DURATION_MS
        )
        VALUES (
            '{run_name}',
            '{meta["table_fqn"]}',
            '{test_name}',
            $$ {sql_used} $$,
            {'TRUE' if passed else 'FALSE'},
            $$ {metrics_json} $$,
            $$ {error_json} $$,
            {duration_ms}
        )
    """).collect()


# ---------------------------------------------------------
# NULL-safe comparison helpers
# ---------------------------------------------------------
def null_safe_eq(col1: str, col2: str) -> str:
    """
    Generates Snowflake NULL-safe equality:
      NVL2(col1, col1, '<NULL>') = NVL2(col2, col2, '<NULL>')
    """
    return f"NVL2({col1}, {col1}, '<NULL>') = NVL2({col2}, {col2}, '<NULL>')"


def null_safe_neq(col1: str, col2: str) -> str:
    """
    Generates Snowflake NULL-safe inequality.
    """
    return f"NVL2({col1}, {col1}, '<NULL>') != NVL2({col2}, {col2}, '<NULL>')"


# ---------------------------------------------------------
# Validate SCD2-required columns
# ---------------------------------------------------------
def validate_scd2_required_columns(meta: Dict):
    """
    Ensures all required SCD2 columns are available.
    Metadata may override defaults.
    """
    default_required = [
        "start_dt",
        "end_dt",
        "business_date",
        "batch_id",
        "last_updated_ts",
    ]

    return meta["table"].get("scd2_required_columns", default_required)
