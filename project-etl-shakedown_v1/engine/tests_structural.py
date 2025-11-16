# =========================================================
# engine/tests_structural.py
# Structural / Integrity Tests (Tests 1–6)
# =========================================================

from typing import Dict
from engine.helpers import (
    run_sql_with_timing,
    insert_result_row,
    validate_scd2_required_columns
)


# =========================================================
# 1. BK_NOT_NULL
# =========================================================
def test_bk_not_null(session, meta: Dict, run_name: str):
    """
    Business Key columns must NOT be NULL.
    """
    fqn = meta["table_fqn"]
    bk_cols = meta["table"].get("bk_columns", [])

    for col in bk_cols:
        sql = f"SELECT COUNT(*) CNT FROM {fqn} WHERE {col} IS NULL"
        rows, dur = run_sql_with_timing(session, sql)
        null_count = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "BK_NOT_NULL",
            sql_used=sql,
            passed=(null_count == 0),
            metrics={"null_count": null_count},
            error=None,
            duration_ms=dur
        )


# =========================================================
# 2. PK_NOT_NULL
# =========================================================
def test_pk_not_null(session, meta: Dict, run_name: str):
    """
    PK columns must NOT be NULL.
    """
    fqn = meta["table_fqn"]
    pk_cols = meta["table"].get("pk_columns", [])

    for col in pk_cols:
        sql = f"SELECT COUNT(*) CNT FROM {fqn} WHERE {col} IS NULL"
        rows, dur = run_sql_with_timing(session, sql)
        null_count = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "PK_NOT_NULL",
            sql_used=sql,
            passed=(null_count == 0),
            metrics={"null_count": null_count},
            error=None,
            duration_ms=dur
        )


# =========================================================
# 3. SCD2_COLS_NOT_NULL
# =========================================================
def test_scd2_cols_not_null(session, meta: Dict, run_name: str):
    """
    All required SCD2 technical audit columns must NOT be NULL:
       start_dt, end_dt, business_date, batch_id, last_updated_ts
    Or the version defined in metadata: scd2_required_columns.
    """
    fqn = meta["table_fqn"]
    required_cols = validate_scd2_required_columns(meta)

    for col in required_cols:
        sql = f"SELECT COUNT(*) CNT FROM {fqn} WHERE {col} IS NULL"
        rows, dur = run_sql_with_timing(session, sql)
        null_count = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "SCD2_COLS_NOT_NULL",
            sql_used=sql,
            passed=(null_count == 0),
            metrics={"null_count": null_count},
            error=None,
            duration_ms=dur
        )


# =========================================================
# 4. SK_UNIQUENESS
# =========================================================
def test_sk_uniqueness(session, meta: Dict, run_name: str):
    """
    Surrogate key must be unique.
    Detect duplicates using:
        COUNT(*) - COUNT(DISTINCT sk)
    """
    fqn = meta["table_fqn"]
    sk = meta["table"].get("surrogate_key", "sk")

    sql = f"""
        SELECT 
            COUNT(*) - COUNT(DISTINCT {sk}) AS DUP
        FROM {fqn}
    """
    rows, dur = run_sql_with_timing(session, sql)
    dup_count = rows[0]["DUP"]

    insert_result_row(
        session, meta, run_name, "SK_UNIQUENESS",
        sql_used=sql,
        passed=(dup_count == 0),
        metrics={"duplicate_sk": dup_count},
        error=None,
        duration_ms=dur
    )


# =========================================================
# 5. NO_FUTURE_DATES
# =========================================================
def test_no_future_dates(session, meta: Dict, run_name: str):
    """
    No record should have start_dt > business_date.
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"""
        SELECT COUNT(*) CNT
        FROM {fqn}
        WHERE start_dt > '{bd}'
    """
    rows, dur = run_sql_with_timing(session, sql)
    future_rows = rows[0]["CNT"]

    insert_result_row(
        session, meta, run_name, "NO_FUTURE_DATES",
        sql_used=sql,
        passed=(future_rows == 0),
        metrics={"future_rows": future_rows},
        error=None,
        duration_ms=dur
    )


# =========================================================
# 6. ETL_BATCH_COLS_NOT_NULL
# =========================================================
def test_etl_batch_cols_not_null(session, meta: Dict, run_name: str):
    """
    All ETL batch/audit columns must NOT be NULL.
    Uses scd2_required_columns to enforce:
      start_dt, end_dt, business_date, batch_id, last_updated_ts
    """
    fqn = meta["table_fqn"]
    required_cols = validate_scd2_required_columns(meta)

    for col in required_cols:
        sql = f"SELECT COUNT(*) CNT FROM {fqn} WHERE {col} IS NULL"
        rows, dur = run_sql_with_timing(session, sql)
        null_count = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "ETL_BATCH_COLS_NOT_NULL",
            sql_used=sql,
            passed=(null_count == 0),
            metrics={"null_count": null_count},
            error=None,
            duration_ms=dur
        )
