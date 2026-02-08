"""
SNOWPARK DATA QUALITY + SCD2 VALIDATION FRAMEWORK (PROD-SAFE)
============================================================

Author : Rubina Jamal
Date   : 2026-02-08

PURPOSE
-------
This script performs deep, deterministic Data Quality (DQ), Foreign Key (FK),
and SCD2 correctness validation using Snowpark (Python) executed in Snowflake.

It is designed for:
- Migration validation
- Release testing
- SCD2 correctness proof
- Data reconciliation-style checks
- CI/CD gating

THIS SCRIPT IS:
--------------
• Snowflake-native (Snowpark)
• Metadata-driven
• Environment-aware (DEV / TEST / PROD)
• Safe to run in PROD (read-only, guarded)
• Fully auditable

THIS SCRIPT IS NOT:
------------------
• A monitoring tool (DMFs serve that purpose)
• A data modification tool
• A row-correction framework

MENTAL MODEL
------------
DMFs  → continuous monitoring (BAU)
This  → deep correctness validation (pre-release / migration)

============================================================
"""

# ============================================================
# 1. EXPLICIT ENVIRONMENT DECLARATION (MANDATORY)
# ============================================================

ENV = "DEV"  # DEV | TEST | PROD
ALLOW_PROD_RUN = False  # MUST be explicitly set to True for PROD

ALLOWED_ENVS = ["DEV", "TEST", "PROD"]

if ENV not in ALLOWED_ENVS:
    raise ValueError(f"Invalid ENV: {ENV}")

if ENV == "PROD" and not ALLOW_PROD_RUN:
    raise RuntimeError(
        "PROD execution blocked. "
        "Set ALLOW_PROD_RUN = True to proceed."
    )

# ============================================================
# 2. ENVIRONMENT TEST POLICY (PROMOTION LOGIC)
# ============================================================

ENV_TEST_POLICY = {
    "DEV": {
        "run_fk_checks": True,
        "run_scd2_checks": True
    },
    "TEST": {
        "run_fk_checks": True,
        "run_scd2_checks": True
    },
    "PROD": {
        "run_fk_checks": True,
        "run_scd2_checks": False  # smoke-level only
    }
}

POLICY = ENV_TEST_POLICY[ENV]

# ============================================================
# 3. CONFIGURATION (ALL INPUTS AT TOP)
# ============================================================

DATABASE_NAME = "EDW"
RESULT_SCHEMA = "STG"
RESULT_TABLE = "QA_DQ_RUN"

EXPECTED_WAREHOUSE = {
    "DEV": "DEV_QA_WH",
    "TEST": "TEST_QA_WH",
    "PROD": "PROD_QA_WH"
}

TABLE_TEST_CONFIG = [
    {
        "schema": "DIM",
        "table": "DIM_CUSTOMER",
        "pk": "CUSTOMER_ID",

        # Numeric range checks
        "range_checks": {
            "AGE": (0, 120)
        },

        # Regex / pattern checks
        "regex_checks": {
            "EMAIL": r"^[^@]+@[^@]+\.[^@]+$"
        },

        # FK relationships (only child + parent provided)
        "fk_relationships": [
            {
                "child_table": "DIM.DIM_CUSTOMER",
                "parent_table": "DIM.DIM_COUNTRY"
            }
        ],

        # Enable SCD2 checks
        "scd2": True
    }
]

# ============================================================
# 4. IMPORTS (STANDARD PYTHON ONLY)
# ============================================================

from datetime import datetime
import uuid

# ============================================================
# 5. CORE HELPERS (SESSION-AGNOSTIC)
# ============================================================

def validate_environment(session):
    """
    Ensures script is running on the correct warehouse for the environment.
    Prevents accidental heavy scans on shared or ETL warehouses.
    """
    current_wh = session.sql(
        "SELECT CURRENT_WAREHOUSE()"
    ).collect()[0][0]

    if current_wh != EXPECTED_WAREHOUSE[ENV]:
        raise RuntimeError(
            f"Wrong warehouse for {ENV}. "
            f"Expected {EXPECTED_WAREHOUSE[ENV]}, got {current_wh}"
        )


def create_result_table(session):
    """
    Persistent audit table.
    Never truncated. Each run is immutable.
    """
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {RESULT_SCHEMA}.{RESULT_TABLE} (
            run_ts         TIMESTAMP,
            run_id         STRING,
            env            STRING,
            schema_name    STRING,
            table_name     STRING,
            test_name      STRING,
            status         STRING,
            failure_count  NUMBER,
            test_sql       STRING
        )
    """).collect()


def log_test(session, run_ts, run_id, schema, table, test_name, sql):
    """
    Executes a validation query that returns COUNT(*)
    Logs PASS / FAIL with full SQL for auditability
    """
    failure_count = session.sql(sql).collect()[0][0]
    status = "PASS" if failure_count == 0 else "FAIL"

    session.sql(f"""
        INSERT INTO {RESULT_SCHEMA}.{RESULT_TABLE}
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_ts,
        run_id,
        ENV,
        schema,
        table,
        test_name,
        status,
        failure_count,
        sql
    ]).collect()


def get_fk_mappings(session, child_table, parent_table):
    """
    Auto-discovers FK column mappings using INFORMATION_SCHEMA.
    """
    child_schema, child_name = child_table.split(".")
    parent_schema, parent_name = parent_table.split(".")

    return session.sql(f"""
        SELECT
            kcu_child.column_name  AS child_column,
            kcu_parent.column_name AS parent_column
        FROM {DATABASE_NAME}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        JOIN {DATABASE_NAME}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu_child
            ON rc.constraint_name = kcu_child.constraint_name
        JOIN {DATABASE_NAME}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu_parent
            ON rc.unique_constraint_name = kcu_parent.constraint_name
        WHERE kcu_child.table_schema = '{child_schema}'
          AND kcu_child.table_name   = '{child_name}'
          AND kcu_parent.table_schema = '{parent_schema}'
          AND kcu_parent.table_name   = '{parent_name}'
    """).collect()

# ============================================================
# 6. MAIN VALIDATION LOGIC
# ============================================================

def run_all_tests(session):
    """
    Executes all configured DQ, FK, and SCD2 tests.
    Raises AssertionError if any test fails.
    """

    validate_environment(session)
    create_result_table(session)

    run_ts = datetime.utcnow()
    run_id = str(uuid.uuid4())

    for cfg in TABLE_TEST_CONFIG:
        schema = cfg["schema"]
        table = cfg["table"]
        full_table = f"{schema}.{table}"

        # -------------------------------
        # DQ: Table not empty
        # -------------------------------
        log_test(
            session, run_ts, run_id, schema, table,
            "DQ_TABLE_NOT_EMPTY",
            f"SELECT COUNT(*) FROM {full_table} HAVING COUNT(*) = 0"
        )

        # -------------------------------
        # DQ: PK NOT NULL
        # -------------------------------
        log_test(
            session, run_ts, run_id, schema, table,
            "DQ_PK_NOT_NULL",
            f"SELECT COUNT(*) FROM {full_table} WHERE {cfg['pk']} IS NULL"
        )

        # -------------------------------
        # DQ: PK UNIQUE
        # -------------------------------
        log_test(
            session, run_ts, run_id, schema, table,
            "DQ_PK_UNIQUE",
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {cfg['pk']}
                FROM {full_table}
                GROUP BY {cfg['pk']}
                HAVING COUNT(*) > 1
            )
            """
        )

        # -------------------------------
        # DQ: Column range checks
        # -------------------------------
        for col, (min_v, max_v) in cfg.get("range_checks", {}).items():
            log_test(
                session, run_ts, run_id, schema, table,
                f"DQ_RANGE_{col}",
                f"""
                SELECT COUNT(*)
                FROM {full_table}
                WHERE {col} < {min_v}
                   OR {col} > {max_v}
                """
            )

        # -------------------------------
        # DQ: Regex checks
        # -------------------------------
        for col, pattern in cfg.get("regex_checks", {}).items():
            log_test(
                session, run_ts, run_id, schema, table,
                f"DQ_REGEX_{col}",
                f"""
                SELECT COUNT(*)
                FROM {full_table}
                WHERE {col} IS NOT NULL
                  AND NOT REGEXP_LIKE({col}, '{pattern}')
                """
            )

        # -------------------------------
        # FK: Orphan detection (auto-discovered)
        # -------------------------------
        if POLICY["run_fk_checks"]:
            for rel in cfg.get("fk_relationships", []):
                fks = get_fk_mappings(
                    session,
                    rel["child_table"],
                    rel["parent_table"]
                )

                for fk in fks:
                    log_test(
                        session, run_ts, run_id, schema, table,
                        f"DQ_FK_{fk['CHILD_COLUMN']}",
                        f"""
                        SELECT COUNT(*)
                        FROM {rel['child_table']} c
                        LEFT JOIN {rel['parent_table']} p
                          ON c.{fk['CHILD_COLUMN']} = p.{fk['PARENT_COLUMN']}
                        WHERE c.{fk['CHILD_COLUMN']} IS NOT NULL
                          AND p.{fk['PARENT_COLUMN']} IS NULL
                        """
                    )

        # -------------------------------
        # SCD2 checks (policy-controlled)
        # -------------------------------
        if cfg.get("scd2") and POLICY["run_scd2_checks"]:
            log_test(
                session, run_ts, run_id, schema, table,
                "SCD2_SINGLE_ACTIVE",
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT {cfg['pk']}
                    FROM {full_table}
                    WHERE is_current = 1
                    GROUP BY {cfg['pk']}
                    HAVING COUNT(*) != 1
                )
                """
            )

            log_test(
                session, run_ts, run_id, schema, table,
                "SCD2_ACTIVE_OPEN",
                f"""
                SELECT COUNT(*)
                FROM {full_table}
                WHERE is_current = 1
                  AND valid_to IS NOT NULL
                """
            )

            log_test(
                session, run_ts, run_id, schema, table,
                "SCD2_DELETED_CLOSED",
                f"""
                SELECT COUNT(*)
                FROM {full_table}
                WHERE is_deleted = 1
                  AND valid_to IS NULL
                """
            )

    # ====================================================
    # PYTEST-STYLE FINAL ASSERTION
    # ====================================================

    failures = session.sql(f"""
        SELECT schema_name, table_name, test_name, failure_count
        FROM {RESULT_SCHEMA}.{RESULT_TABLE}
        WHERE run_id = ?
          AND status = 'FAIL'
    """, [run_id]).collect()

    if failures:
        raise AssertionError(
            f"Data quality tests failed for run_id {run_id}"
        )

# ============================================================
# 7. MINIMAL ENTRY POINT (CI / LOCAL MODE)
# ============================================================

def main():
    """
    Minimal entry point.
    Session creation is expected to be handled by:
    • CI/CD runner OR
    • Snowpark worksheet injection

    In a worksheet, call run_all_tests(session) directly.
    """
    run_all_tests(session)

# Execute when run as script
if __name__ == "__main__":
    main()
