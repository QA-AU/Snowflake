# =========================================================
# engine/registry.py
# Central Test Registry for all 23 Tests
# =========================================================

# Import all test modules
from engine.tests_structural import (
    test_bk_not_null,
    test_pk_not_null,
    test_scd2_cols_not_null,
    test_sk_uniqueness,
    test_no_future_dates,
    test_etl_batch_cols_not_null
)

from engine.tests_scd2_temporal import (
    test_scd2_start_date_valid,
    test_scd2_end_date_valid,
    test_scd2_only_one_active,
    test_scd2_change_detection,
    test_scd2_effective_sequence,
    test_scd2_surrogate_reuse
)

from engine.tests_business import (
    test_business_date_max_match,
    test_insert_count,
    test_update_count,
    test_delete_count
)

from engine.tests_duplicates import (
    test_duplicate_rows_bk,
    test_duplicate_rows_bk_scd2
)

from engine.tests_quality import (
    test_col_allowed_values,
    test_col_whitespace_clean,
    test_col_range_validation
)

from engine.tests_relational import (
    test_fk_relation_check,
    test_orphan_records_check
)


# =========================================================
# Test Registry Mapping
# =========================================================
# Keys correspond to test names used in metadata["tests_to_run"]
# Values are function references imported above.

TEST_REGISTRY = {
    # Structural Tests (1–6)
    "BK_NOT_NULL": test_bk_not_null,
    "PK_NOT_NULL": test_pk_not_null,
    "SCD2_COLS_NOT_NULL": test_scd2_cols_not_null,
    "SK_UNIQUENESS": test_sk_uniqueness,
    "NO_FUTURE_DATES": test_no_future_dates,
    "ETL_BATCH_COLS_NOT_NULL": test_etl_batch_cols_not_null,

    # SCD2 Temporal Tests (7–12)
    "SCD2_START_DATE_VALID": test_scd2_start_date_valid,
    "SCD2_END_DATE_VALID": test_scd2_end_date_valid,
    "SCD2_ONLY_ONE_ACTIVE": test_scd2_only_one_active,
    "SCD2_CHANGE_DETECTION": test_scd2_change_detection,
    "SCD2_EFFECTIVE_SEQUENCE": test_scd2_effective_sequence,
    "SCD2_SURROGATE_REUSE": test_scd2_surrogate_reuse,

    # Business-Date & Load Activity Tests (13–16)
    "BUSINESS_DATE_MAX_MATCH": test_business_date_max_match,
    "INSERT_COUNT": test_insert_count,
    "UPDATE_COUNT": test_update_count,
    "DELETE_COUNT": test_delete_count,

    # Duplicate Checks (17–18)
    "DUPLICATE_ROWS_BK": test_duplicate_rows_bk,
    "DUPLICATE_ROWS_BK_SCD2": test_duplicate_rows_bk_scd2,

    # Quality Tests (19–21)
    "COL_ALLOWED_VALUES": test_col_allowed_values,
    "COL_WHITESPACE_CLEAN": test_col_whitespace_clean,
    "COL_RANGE_VALIDATION": test_col_range_validation,

    # Relational Integrity Tests (22–23)
    "FK_RELATION_CHECK": test_fk_relation_check,
    "ORPHAN_RECORDS_CHECK": test_orphan_records_check,
}

# =========================================================
# END OF registry.py
# =========================================================
