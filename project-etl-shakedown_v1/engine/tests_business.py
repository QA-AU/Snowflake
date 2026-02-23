# =========================================================
# engine/tests_business.py
# Business-Date & Load-Activity Tests (Tests 13–16)
# =========================================================

from typing import Dict
from engine.helpers import run_sql_with_timing, insert_result_row


# =========================================================
# 13. BUSINESS_DATE_MAX_MATCH
# =========================================================
def test_business_date_max_match(session, meta: Dict, run_name: str):
    """
    Validates that MAX(business_date) in the table
    equals the current run business_date.
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"SELECT MAX(business_date) AS MX FROM {fqn}"

    rows, dur = run_sql_with_timing(session, sql)
    max_bd = str(rows[0]["MX"]) if rows[0]["MX"] else None

    passed = max_bd == bd

    insert_result_row(
        session,
        meta,
        run_name,
        "BUSINESS_DATE_MAX_MATCH",
        sql_used=sql,
        passed=passed,
        metrics={"max_business_date": max_bd, "expected_business_date": bd},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 14. INSERT_COUNT
# =========================================================
def test_insert_count(session, meta: Dict, run_name: str):
    """
    Counts newly inserted SCD2 rows:
        start_dt = business_date
    Inserts metric only. Does not enforce pass/fail rule.
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"""
        SELECT COUNT(*) AS CNT
        FROM {fqn}
        WHERE start_dt = '{bd}'
    """

    rows, dur = run_sql_with_timing(session, sql)
    count = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "INSERT_COUNT",
        sql_used=sql,
        passed=True,
        metrics={"inserted_rows": count},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 15. UPDATE_COUNT
# =========================================================
def test_update_count(session, meta: Dict, run_name: str):
    """
    Counts SCD2 updates where:
        end_dt = business_date - 1
    Only provides metric (does not enforce pass/fail).
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"""
        SELECT COUNT(*) AS CNT
        FROM {fqn}
        WHERE end_dt = DATEADD(day, -1, '{bd}')
    """

    rows, dur = run_sql_with_timing(session, sql)
    count = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "UPDATE_COUNT",
        sql_used=sql,
        passed=True,
        metrics={"updated_rows": count},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 16. DELETE_COUNT
# =========================================================
def test_delete_count(session, meta: Dict, run_name: str):
    """
    Counts logically deleted rows where:
        logical_delete_date = business_date - 1
        AND end_dt != '9999-12-31'
    Only provides metric.
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"""
        SELECT COUNT(*) CNT
        FROM {fqn}
        WHERE logical_delete_date = DATEADD(day, -1, '{bd}')
          AND end_dt != '9999-12-31'
    """

    rows, dur = run_sql_with_timing(session, sql)
    count = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "DELETE_COUNT",
        sql_used=sql,
        passed=True,
        metrics={"deleted_rows": count},
        error=None,
        duration_ms=dur,
    )
