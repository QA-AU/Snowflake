# =============================================================================
# Function: ingest_file_snowpark
#
# Description:
# -----------------------------------------------------------------------------
# Universal, metadata-driven ingestion function built using Snowflake Snowpark.
# This function ingests large files from an external stage into Snowflake
# WITHOUT using COPY, pandas, connectors, or stored procedures.
#
# The function is designed for legacy and mixed file formats and enforces a
# SINGLE structural validation rule:
#
#   ➤ The number of columns parsed per row MUST exactly match the number of
#     columns provided in the external header list.
#
# Content validation (NULLs, empty strings, "", whitespace) is intentionally
# NOT performed at this stage. All columns are ingested as STRING.
#
# Key Characteristics:
# -----------------------------------------------------------------------------
# • Headers are ALWAYS provided externally and NEVER read from the file
# • Files may contain header rows — these are treated as data and rejected
# • Supports DELIMITED and FIXED-WIDTH files
# • Handles legacy UTF-8 delimiters (e.g. inverted question mark ¿)
# • Normalizes unsafe delimiters to a safe internal delimiter before parsing
# • Uses Snowpark DataFrame operations only (no raw INSERT SQL)
# • Always CREATE OR REPLACE target and reject tables (no append)
# • Captures rejected rows with raw data preserved
# • Emits telemetry and a human-readable status message
#
# Validation Rule (ONLY ONE):
# -----------------------------------------------------------------------------
# A row is VALID if:
#   size(parsed_columns) == size(header_list)
#
# A row is REJECTED if:
#   size(parsed_columns) != size(header_list)
#
# NULL, empty strings, and "" are all allowed values.
#
# Tables Created:
# -----------------------------------------------------------------------------
# 1. <target_table>
#    - One column per header_list entry
#    - All columns STRING
#
# 2. <target_table>_REJECT
#    - raw_row                 STRING
#    - actual_column_count     STRING
#    - expected_column_count   STRING
#    - reject_reason           STRING
#    - reject_ts               TIMESTAMP
#
# Input Parameters:
# -----------------------------------------------------------------------------
# session           : Snowpark session (auto-provided in Python Worksheets)
# stage_path        : External stage path (e.g. "@ext_stage/path/file")
# header_list       : Ordered list of column names (schema contract)
# target_table      : Fully-qualified target table name (schema.table)
# file_type         : "DELIMITED" or "FIXED"
# fixed_widths      : List of field widths (required if file_type="FIXED")
# row_delimiter     : Row delimiter (default = "\n")
# legacy_delimiter  : Legacy delimiter to normalize (default = "¿")
# safe_delimiter    : Internal delimiter used for parsing (default = ASCII 0x1F)
#
# Returns:
# -----------------------------------------------------------------------------
# telemetry (dict)  : Execution metrics and run status
# message   (str)   : Human-readable success or error message
#
# =============================================================================

from snowflake.snowpark.functions import (
    col, split, size, replace, lit, current_timestamp,
    substring, array_construct
)
from snowflake.snowpark.types import StringType


def ingest_file_snowpark(
    session,
    stage_path: str,
    header_list: list,
    target_table: str,
    file_type: str,                 # "DELIMITED" | "FIXED"
    fixed_widths: list = None,       # required if FIXED
    row_delimiter: str = "\n",
    legacy_delimiter: str = "¿",
    safe_delimiter: str = "\x1F"
):

    reject_table = f"{target_table}_REJECT"
    expected_cols = len(header_list)

    print("====================================")
    print(" INGESTION STARTED")
    print("====================================")

    try:
        # 1. Read raw file as single-column rows
        raw_df = (
            session.read
                .option("RECORD_DELIMITER", row_delimiter)
                .csv(stage_path)
        )

        total_rows = raw_df.count()

        # 2. Parse rows based on file type
        if file_type == "DELIMITED":
            parsed_df = raw_df.select(
                split(
                    replace(col("$1"), legacy_delimiter, safe_delimiter),
                    safe_delimiter
                ).alias("cols"),
                col("$1").alias("raw_row")
            )

        elif file_type == "FIXED":
            if not fixed_widths or len(fixed_widths) != expected_cols:
                raise ValueError("fixed_widths must match header_list length")

            pos = 1
            col_exprs = []
            for width in fixed_widths:
                col_exprs.append(substring(col("$1"), pos, width))
                pos += width

            parsed_df = raw_df.select(
                array_construct(*col_exprs).alias("cols"),
                col("$1").alias("raw_row")
            )

        else:
            raise ValueError("file_type must be 'DELIMITED' or 'FIXED'")

        # 3. Apply column-count validation rule
        valid_df = parsed_df.filter(size(col("cols")) == expected_cols)
        reject_df = parsed_df.filter(size(col("cols")) != expected_cols)

        # 4. Project valid rows
        valid_out = valid_df.select(
            *[
                col("cols")[i].cast(StringType()).alias(header_list[i])
                for i in range(expected_cols)
            ]
        )

        # 5. Replace target table
        valid_out.write.mode("overwrite").save_as_table(target_table)

        # 6. Replace reject table
        reject_out = reject_df.select(
            col("raw_row").cast(StringType()).alias("raw_row"),
            size(col("cols")).cast(StringType()).alias("actual_column_count"),
            lit(str(expected_cols)).alias("expected_column_count"),
            lit("COLUMN_COUNT_MISMATCH").alias("reject_reason"),
            current_timestamp().alias("reject_ts")
        )

        reject_out.write.mode("overwrite").save_as_table(reject_table)

        telemetry = {
            "stage_path": stage_path,
            "target_table": target_table,
            "reject_table": reject_table,
            "file_type": file_type,
            "expected_column_count": expected_cols,
            "total_rows": total_rows,
            "valid_rows": valid_df.count(),
            "reject_rows": reject_df.count(),
            "status": "SUCCESS" if reject_df.count() == 0 else "PARTIAL_SUCCESS"
        }

        print("====================================")
        print(" INGESTION COMPLETED")
        print("====================================")

        return telemetry, "Ingestion completed successfully"

    except Exception as e:
        print("====================================")
        print(" INGESTION FAILED")
        print("====================================")

        return {
            "stage_path": stage_path,
            "target_table": target_table,
            "status": "ERROR",
            "error_message": str(e)
        }, f"Ingestion failed: {e}"
