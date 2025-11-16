# =========================================================
# engine/tests_relational.py
# Relational Integrity Tests (Tests 22–23)
# =========================================================

from typing import Dict
from engine.helpers import (
    run_sql_with_timing,
    insert_result_row
)


# =========================================================
# 22. FK_RELATION_CHECK
# =========================================================
def test_fk_relation_check(session, meta: Dict, run_name: str):
    """
    Ensures foreign-key references from the SCD2 table to a parent table
    are valid.

    Metadata format:
        "fk_relations": [
            {
                "child_column": "CUST_ID",
                "parent_table": "CORE.CUSTOMER_DIM",
                "parent_column": "CUST_ID"
            }
        ]

    NULL child FK values are allowed.
    Only open parent rows (end_dt = '9999-12-31') are considered valid.
    """
    fqn = meta["table_fqn"]
    fk_cfg = meta["table"].get("fk_relations", [])

    if not fk_cfg:
        return  # No FK tests configured

    for fk in fk_cfg:

        child_col = fk["child_column"]
        parent_table = fk["parent_table"]
        parent_col = fk["parent_column"]

        sql = f"""
            SELECT COUNT(*) CNT
            FROM {fqn} c
            WHERE c.{child_col} IS NOT NULL
              AND c.{child_col} NOT IN (
                    SELECT p.{parent_col}
                    FROM {parent_table} p
                    WHERE p.end_dt = '9999-12-31'
              )
        """

        rows, dur = run_sql_with_timing(session, sql)
        mismatch = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "FK_RELATION_CHECK",
            sql_used=sql,
            passed=(mismatch == 0),
            metrics={
                "fk_mismatch_count": mismatch,
                "child_column": child_col,
                "parent_table": parent_table,
                "parent_column": parent_col,
            },
            error=None,
            duration_ms=dur
        )


# =========================================================
# 23. ORPHAN_RECORDS_CHECK
# =========================================================
def test_orphan_records_check(session, meta: Dict, run_name: str):
    """
    Detects FACT → DIM orphan references.
    
    Metadata format:
        "orphan_checks": [
            {
                "fact_table": "CORE.SALES_FACT",
                "fact_column": "PRODUCT_ID",
                "dim_table": "CORE.PRODUCT_DIM",
                "dim_column": "PRODUCT_ID"
            }
        ]

    Orphans occur when FACT.fact_column does not match any open DIM.dim_column.
    """
    orphan_cfg = meta["table"].get("orphan_checks", [])

    if not orphan_cfg:
        return  # No orphan tests configured

    for cfg in orphan_cfg:

        fact_table = cfg["fact_table"]
        fact_col = cfg["fact_column"]
        dim_table = cfg["dim_table"]
        dim_col = cfg["dim_column"]

        sql = f"""
            SELECT COUNT(*) CNT
            FROM {fact_table} f
            WHERE f.{fact_col} IS NOT NULL
              AND f.{fact_col} NOT IN (
                    SELECT d.{dim_col}
                    FROM {dim_table} d
                    WHERE d.end_dt = '9999-12-31'
              )
        """

        rows, dur = run_sql_with_timing(session, sql)
        orphan_count = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "ORPHAN_RECORDS_CHECK",
            sql_used=sql,
            passed=(orphan_count == 0),
            metrics={
                "orphan_count": orphan_count,
                "fact_table": fact_table,
                "dim_table": dim_table,
                "fact_column": fact_col,
                "dim_column": dim_col,
            },
            error=None,
            duration_ms=dur
        )
