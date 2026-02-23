"""
Great Expectations   Data Quality & SCD2 Validation

PURPOSE
-------
This script performs deep, deterministic data quality validation
using Great Expectations against Snowflake tables.

It is designed to:
- Run from CI/CD or local Python runtime
- Act as a release gate (pass/fail)
- Validate structural, business, referential, and SCD2 correctness

NOT DESIGNED FOR
----------------
- Snowflake Worksheets
- Snowpark Stored Procedures
- Snowflake Tasks

RATIONALE
---------
Great Expectations requires an external Python runtime,
filesystem access, and lifecycle management which Snowflake
does not provide internally.
"""

import os
import sys
from great_expectations.data_context import DataContext

# ============================================================
# 1. ENVIRONMENT CONFIGURATION
# ============================================================
# Controls which Snowflake environment is validated.
# Validation logic is identical across environments.

ENV = os.getenv("ENV", "DEV")  # DEV | TEST | PROD

SNOWFLAKE_ENV = {
    "DEV": {
        "ACCOUNT": "acct_dev",
        "DATABASE": "EDW_DEV",
        "SCHEMA": "DIM",
        "USER": "svc_ge_dev",
        "PASSWORD": os.getenv("SF_DEV_PWD"),
        "WAREHOUSE": "WH_DEV",
    },
    "TEST": {
        "ACCOUNT": "acct_test",
        "DATABASE": "EDW_TEST",
        "SCHEMA": "DIM",
        "USER": "svc_ge_test",
        "PASSWORD": os.getenv("SF_TEST_PWD"),
        "WAREHOUSE": "WH_TEST",
    },
    "PROD": {
        "ACCOUNT": "acct_prod",
        "DATABASE": "EDW",
        "SCHEMA": "DIM",
        "USER": "svc_ge_prod",
        "PASSWORD": os.getenv("SF_PROD_PWD"),
        "WAREHOUSE": "WH_PROD",
    },
}[ENV]

# ============================================================
# 2. TABLE METADATA (TEST INPUTS)
# ============================================================
# This section defines WHAT is tested.
# The framework is metadata-driven and scalable.

TABLES = [
    {
        "asset": "dim_customer",
        "table": "DIM_CUSTOMER",
        "pk": "customer_id",
        # Column range checks (DQ-COL-001)
        "ranges": {"age": (0, 120)},
        # Regex / pattern checks (DQ-COL-002)
        "regex": {"email": r"^[^@]+@[^@]+\.[^@]+$"},
        # Foreign key integrity checks (DQ-FK-001)
        "foreign_keys": [
            {
                "column": "country_id",
                "ref_table": "DIM_COUNTRY",
                "ref_column": "country_id",
            }
        ],
    }
]

# ============================================================
# 3. GREAT EXPECTATIONS CONTEXT & DATASOURCE
# ============================================================

context = DataContext()

sf_source = context.sources.add_or_update_snowflake(
    name="sf_src",
    connection_string=(
        f"snowflake://{SNOWFLAKE_ENV['USER']}:"
        f"{SNOWFLAKE_ENV['PASSWORD']}@"
        f"{SNOWFLAKE_ENV['ACCOUNT']}/"
        f"{SNOWFLAKE_ENV['DATABASE']}/"
        f"{SNOWFLAKE_ENV['SCHEMA']}?"
        f"warehouse={SNOWFLAKE_ENV['WAREHOUSE']}"
    ),
)

# Register table assets (idempotent metadata registration)
for table in TABLES:
    sf_source.add_table_asset(
        name=table["asset"],
        table_name=table["table"],
        schema_name=SNOWFLAKE_ENV["SCHEMA"],
    )

# ============================================================
# 4. VALIDATION HELPERS
# ============================================================


def get_validator(asset_name):
    """
    Creates a validator bound to a specific Snowflake table.

    The validator already knows:
    - Account
    - Database
    - Schema
    - Table
    """
    batch_request = sf_source.get_asset(asset_name).build_batch_request()
    return context.get_validator(batch_request=batch_request)


def run_standard_dq_checks(validator, meta):
    """
    STANDARD DATA QUALITY TEST CASES
    """

    pk = meta["pk"]

    # --------------------------------------------------------
    # TEST CASE ID: DQ-TBL-001
    # Title: Table must not be empty
    # DQ Dimension: Completeness
    # Pass Criteria: Row count > 0
    # Fail Impact: Load failure, truncation, or bad filter
    # --------------------------------------------------------
    validator.expect_table_row_count_to_be_greater_than(0)

    # --------------------------------------------------------
    # TEST CASE ID: DQ-PK-001
    # Title: Primary key must not be NULL
    # DQ Dimension: Completeness, Validity
    # Pass Criteria: No NULLs in PK column
    # Fail Impact: Broken joins and SCD2 logic
    # --------------------------------------------------------
    validator.expect_column_values_to_not_be_null(pk)

    # --------------------------------------------------------
    # TEST CASE ID: DQ-PK-002
    # Title: Primary key must be unique
    # DQ Dimension: Uniqueness, Integrity
    # Pass Criteria: All PK values unique
    # Fail Impact: Duplicate entities, incorrect analytics
    # --------------------------------------------------------
    validator.expect_column_values_to_be_unique(pk)

    # --------------------------------------------------------
    # TEST CASE ID: DQ-COL-001
    # Title: Numeric values must be within allowed range
    # DQ Dimension: Validity, Accuracy
    # --------------------------------------------------------
    for col, (min_val, max_val) in meta["ranges"].items():
        validator.expect_column_values_to_be_between(col, min_val, max_val)

    # --------------------------------------------------------
    # TEST CASE ID: DQ-COL-002
    # Title: Column values must match defined pattern
    # DQ Dimension: Conformance
    # --------------------------------------------------------
    for col, pattern in meta["regex"].items():
        validator.expect_column_values_to_match_regex(col, pattern)


def run_fk_integrity_checks(validator, meta):
    """
    FOREIGN KEY DATA QUALITY TEST CASES
    """

    for fk in meta.get("foreign_keys", []):

        child_col = fk["column"]
        parent_table = fk["ref_table"]
        parent_col = fk["ref_column"]

        # --------------------------------------------------------
        # TEST CASE ID: DQ-FK-001
        # Title: Foreign key must not have orphan records
        # DQ Dimension: Referential Integrity, Consistency
        # Pass Criteria: Zero orphan FK values
        # Fail Impact: Broken joins and incomplete analytics
        # --------------------------------------------------------
        validator.expect_query_result_to_be_zero(f"""
            SELECT COUNT(*)
            FROM {{table}} c
            LEFT JOIN {parent_table} p
              ON c.{child_col} = p.{parent_col}
            WHERE c.{child_col} IS NOT NULL
              AND p.{parent_col} IS NULL
        """)


def run_scd2_checks(validator):
    """
    SCD2 DATA QUALITY TEST CASES
    """

    # --------------------------------------------------------
    # TEST CASE ID: DQ-SCD2-001
    # Title: Exactly one active record per natural key
    # DQ Dimension: Consistency, Historical Integrity
    # --------------------------------------------------------
    validator.expect_query_result_to_be_zero("""
        SELECT COUNT(*) FROM (
            SELECT customer_id
            FROM {{table}}
            WHERE is_current = 1
            GROUP BY customer_id
            HAVING COUNT(*) != 1
        )
    """)

    # --------------------------------------------------------
    # TEST CASE ID: DQ-SCD2-002
    # Title: Active record must be open-ended
    # DQ Dimension: Temporal Validity
    # --------------------------------------------------------
    validator.expect_query_result_to_be_zero("""
        SELECT COUNT(*)
        FROM {{table}}
        WHERE is_current = 1
          AND valid_to IS NOT NULL
    """)

    # --------------------------------------------------------
    # TEST CASE ID: DQ-SCD2-003
    # Title: Deleted records must be closed-dated
    # DQ Dimension: Historical Accuracy, Auditability
    # --------------------------------------------------------
    validator.expect_query_result_to_be_zero("""
        SELECT COUNT(*)
        FROM {{table}}
        WHERE is_deleted = 1
          AND valid_to IS NULL
    """)


# ============================================================
# 5. EXECUTION LOOP
# ============================================================

overall_success = True

for table in TABLES:
    print(f"Running data quality tests for table: {table['table']} (ENV={ENV})")

    validator = get_validator(table["asset"])

    run_standard_dq_checks(validator, table)
    run_fk_integrity_checks(validator, table)
    run_scd2_checks(validator)

    results = validator.validate()

    if not results["success"]:
        overall_success = False
        print(f"Data quality tests failed for table: {table['table']}")
    else:
        print(f"Data quality tests passed for table: {table['table']}")

# ============================================================
# 6. CI/CD EXIT STATUS
# ============================================================

if not overall_success:
    print("One or more data quality test cases failed")
    sys.exit(1)

print("All data quality test cases passed")
sys.exit(0)
