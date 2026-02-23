# (You can call load_metadata(session, metadata_json) from main.)

# Step 1


# """
# metadata_loader.py
# Fully functional metadata loader for Snowflake Python Worksheets.

# Purpose:
# - Take metadata as Python dict or JSON string
# - Validate required structures
# - Store metadata into a temp table META_INPUT
# - Keep engine_input and test_input cleanly separated
# """

from Snowflake.snowpark import Session
import json
from typing import Dict, Any, Optional

TABLE_TEST_META = {
    "CORE.ORDERS": {
        "parent_db": "SESAME",
        "business_date": "2025-11-13",
        "debug_mode": "YES",
        "version": "1.0.0",
        "table": {
            "schema": "CORE",
            "name": "ORDERS",
            "pk_columns": ["ORDER_ID", "LINE_NO"],
            "business_date_column": "BUSINESS_DATE",
            "date_columns": ["ORDER_DATE", "SHIP_DATE"],
            "timestamp_columns": ["CREATED_AT", "UPDATED_AT"],
            "trim_columns": ["CUSTOMER_NAME", "ADDRESS"],
            "clean_columns": ["NOTES", "COMMENTS"],
            "scd": {
                "natural_key_columns": ["CUSTOMER_ID"],
                "start_date_column": "VALID_FROM",
                "end_date_column": "VALID_TO",
                "current_flag_column": "IS_CURRENT",
                "open_end_value": "9999-12-31",
            },
            "fk_relations": [
                {
                    "fk_name": "FK_ORDERS_CUSTOMER",
                    "child_column": "CUSTOMER_ID",
                    "parent_schema": "CORE",
                    "parent_table": "CUSTOMERS",
                    "parent_key_column": "CUSTOMER_ID",
                }
            ],
            "extra_filter": "STATUS = 'OPEN'",
        },
        "tests_to_run": [
            "BUSINESS_DATE_MATCH",
            "NON_ZERO_COUNT_FOR_BUSINESS_DATE",
            "PK_NULL_CHECK",
            "STRUCTURAL_DUPLICATES",
            "DATE_COLS_NOT_NULL",
            "TIMESTAMP_COLS_NOT_NULL",
            "TRIMMED_COLS",
            "CLEANED_COLS",
            "SCD2_SINGLE_OPEN_RECORD",
            "FOREIGN_KEY_ORPHANS",
        ],
    }
}


# ================================================================
# INTERNAL HELPERS
# ================================================================


def _validate_metadata(md: Dict[str, Any]):
    """
    Validate that the metadata has expected structure.
    You can expand this if new fields are added.
    """

    required_top = ["parent_db", "table", "tests_to_run"]
    for rt in required_top:
        if rt not in md:
            raise ValueError(f"Missing required metadata key: {rt}")

    table_req = ["schema", "name"]
    for t in table_req:
        if t not in md["table"]:
            raise ValueError(f"Missing 'table.{t}' in metadata")

    # Optional but recommended fields
    # business_date, date_filter, debug_mode, version, etc.


def _safe_json(v: Any) -> str:
    """Convert dict/list/string safely into escaped JSON string."""
    js = json.dumps(v)
    return js.replace("'", "''")  # escape for Snowflake INSERT


# ================================================================
# CREATE TEMP TABLE (only once)   TABLE_TEST_META
# ================================================================


def _ensure_meta_table(session: Session):
    """
    TEMP table META_INPUT stores metadata as KEY, VALUE (VARIANT).
    Multiple runs can overwrite it.
    """
    session.sql("""
        CREATE OR REPLACE TEMP TABLE META_INPUT (
            KEY   STRING,
            VALUE VARIANT
        )
    """).collect()


# ================================================================
# MAIN LOADER
# ================================================================


def load_metadata(session: Session, metadata: Dict[str, Any]):
    """
    Load metadata into META_INPUT.
    - Accepts dict
    - Converts nested structures to VARIANT
    - Can be reused before each engine run

    Usage:
        metadata = {
            "parent_db": "SESAME",
            "business_date": "2025-11-13",
            "debug_mode": "YES",
            "tests_to_run": [...],
            "table": {...}
        }

        load_metadata(session, metadata)
    """

    # Validate structure before loading
    _validate_metadata(metadata)

    # Ensure TEMP table exists
    _ensure_meta_table(session)

    # Clear previous metadata
    session.sql("DELETE FROM META_INPUT").collect()

    # Insert metadata into rows
    for key, value in metadata.items():
        js = _safe_json(value)
        sql = f"""
            INSERT INTO META_INPUT (KEY, VALUE)
            VALUES ('{key}', PARSE_JSON('{js}'))
        """
        session.sql(sql).collect()

    return session.table("META_INPUT")
