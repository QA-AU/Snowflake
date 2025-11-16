# =========================================================
# engine/tests_duplicates.py
# Duplicate Checks (Tests 17–18)
# =========================================================

from typing import Dict
from engine.helpers import (
    run_sql_with_timing,
    insert_result_row
)


# =========================================================
# 17. DUPLICATE_ROWS_BK
# =========================================================
def test_duplicate_rows_bk(session, meta: Dict, run_name: str):
    """
    Detect duplicate Business Key entries.
    
    A BK should uniquely identify a logical business entity.
    If a BK repeats more than once, it indicates data quality issues
    or incorrect SCD2 processing.
    """
    fqn = meta["table_fqn"]
    bk_cols = meta["table"]["bk_columns"]

    if not bk_cols:
        insert_result_row(
            session, meta, run_name, "DUPLICATE_ROWS_BK",
            sql_used="",
            passed=False,
            metrics={},
            error="Metadata missing bk_columns for duplicate check.",
            duration_ms=0
        )
        return

    group_by = ",".join(bk_cols)

    sql = f"""
        SELECT COUNT(*) CNT
        FROM (
            SELECT {group_by}, COUNT(*) AS C
            FROM {fqn}
            GROUP BY {group_by}
            HAVING COUNT(*) > 1
        )
    """

    rows, dur = run_sql_with_timing(session, sql)
    dup_groups = rows[0]["CNT"]

    insert_result_row(
        session, meta, run_name, "DUPLICATE_ROWS_BK",
        sql_used=sql,
        passed=(dup_groups == 0),
        metrics={"duplicate_groups": dup_groups},
        error=None,
        duration_ms=dur
    )


# =========================================================
# 18. DUPLICATE_ROWS_BK_SCD2
# =========================================================
def test_duplicate_rows_bk_scd2(session, meta: Dict, run_name: str):
    """
    Detect duplicates across BK + SCD2 attribute columns.

    Validates that each historical version of a BK differs
    appropriately on the SCD2 attribute columns and that no two rows
    share the same BK *and* identical SCD2 attributes unless required.

    This check is independent of start_dt/end_dt sequencing.
    """
    fqn = meta["table_fqn"]
    bk_cols = meta["table"]["bk_columns"]
    scd_cols = meta["table"].get("scd2_columns", [])

    if not bk_cols:
        insert_result_row(
            session, meta, run_name, "DUPLICATE_ROWS_BK_SCD2",
            sql_used="",
            passed=False,
            metrics={},
            error="Metadata missing bk_columns for SCD2 duplicate check.",
            duration_ms=0
        )
        return

    if not scd_cols:
        insert_result_row(
            session, meta, run_name, "DUPLICATE_ROWS_BK_SCD2",
            sql_used="",
            passed=False,
            metrics={},
            error="Metadata missing scd2_columns for SCD2 duplicate check.",
            duration_ms=0
        )
        return

    # Combined grouping
    group_cols = bk_cols + scd_cols
    group_by = ",".join(group_cols)

    sql = f"""
        SELECT COUNT(*) CNT
        FROM (
            SELECT {group_by}, COUNT(*) AS C
            FROM {fqn}
            GROUP BY {group_by}
            HAVING COUNT(*) > 1
        )
    """

    rows, dur = run_sql_with_timing(session, sql)
    dup_groups = rows[0]["CNT"]

    insert_result_row(
        session, meta, run_name, "DUPLICATE_ROWS_BK_SCD2",
        sql_used=sql,
        passed=(dup_groups == 0),
        metrics={"duplicate_groups": dup_groups},
        error=None,
        duration_ms=dur
    )
