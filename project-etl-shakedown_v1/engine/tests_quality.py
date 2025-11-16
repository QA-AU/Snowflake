# =========================================================
# engine/tests_quality.py
# Column Quality Tests (Tests 19–21)
# =========================================================

from typing import Dict
from engine.helpers import (
    run_sql_with_timing,
    insert_result_row
)


# =========================================================
# 19. COL_ALLOWED_VALUES
# =========================================================
def test_col_allowed_values(session, meta: Dict, run_name: str):
    """
    Validates that each configured column only contains values from
    a specified allowed list (including optional NULL).
    
    Metadata format:
        "allowed_values": {
            "STATUS": ["A", "I", null],
            "TYPE": ["X", "Y"]
        }
    """
    fqn = meta["table_fqn"]
    allowed_cfg = meta["table"].get("allowed_values", {})

    for col, allowed_list in allowed_cfg.items():
        # Build list of values ('X', 'Y', NULL)
        formatted = ",".join(
            ["NULL" if v is None else f"'{v}'" for v in allowed_list]
        )

        sql = f"""
            SELECT COUNT(*) AS CNT
            FROM {fqn}
            WHERE {col} NOT IN ({formatted})
              AND {col} IS NOT NULL
        """

        rows, dur = run_sql_with_timing(session, sql)
        invalid = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "COL_ALLOWED_VALUES",
            sql_used=sql,
            passed=(invalid == 0),
            metrics={"invalid_rows": invalid, "column": col},
            error=None,
            duration_ms=dur
        )


# =========================================================
# 20. COL_WHITESPACE_CLEAN
# =========================================================
def test_col_whitespace_clean(session, meta: Dict, run_name: str):
    """
    Validates that specified columns do NOT contain:
        - leading/trailing spaces
        - newline (\n)
        - carriage return (\r)
        - tabs (\t)
    
    Metadata:
        "whitespace_columns": ["NAME", "DESC"]
    """
    fqn = meta["table_fqn"]
    cols = meta["table"].get("whitespace_columns", [])

    for col in cols:
        sql = f"""
            SELECT COUNT(*) AS CNT
            FROM {fqn}
            WHERE {col} != TRIM({col})
               OR {col} LIKE '%\\n%'
               OR {col} LIKE '%\\r%'
               OR {col} LIKE '%\\t%'
        """

        rows, dur = run_sql_with_timing(session, sql)
        issues = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "COL_WHITESPACE_CLEAN",
            sql_used=sql,
            passed=(issues == 0),
            metrics={"bad_whitespace_rows": issues, "column": col},
            error=None,
            duration_ms=dur
        )


# =========================================================
# 21. COL_RANGE_VALIDATION
# =========================================================
def test_col_range_validation(session, meta: Dict, run_name: str):
    """
    Validates that numeric values fall within configured min/max ranges.
    
    Metadata:
        "numeric_ranges": {
            "AMOUNT": {"min": 0, "max": 999999.99},
            "QUANTITY": {"min": 0, "max": 10000}
        }
    """
    fqn = meta["table_fqn"]
    ranges = meta["table"].get("numeric_ranges", {})

    for col, cfg in ranges.items():
        min_val = cfg.get("min")
        max_val = cfg.get("max")

        sql = f"""
            SELECT COUNT(*) AS CNT
            FROM {fqn}
            WHERE {col} < {min_val}
               OR {col} > {max_val}
        """

        rows, dur = run_sql_with_timing(session, sql)
        bad_rows = rows[0]["CNT"]

        insert_result_row(
            session, meta, run_name, "COL_RANGE_VALIDATION",
            sql_used=sql,
            passed=(bad_rows == 0),
            metrics={"out_of_range": bad_rows, "column": col},
            error=None,
            duration_ms=dur
        )
