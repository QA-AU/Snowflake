


CREATE OR REPLACE PROCEDURE UTIL.INGEST_FILE_SP(
    STAGE_PATH STRING,
    HEADER_LIST ARRAY,
    TARGET_TABLE STRING,
    FILE_TYPE STRING,          -- 'DELIMITED' or 'FIXED'
    FIXED_WIDTHS ARRAY,        -- NULL unless FIXED
    ROW_DELIMITER STRING,      -- e.g. '\n'
    LEGACY_DELIMITER STRING,   -- e.g. '¿'
    SAFE_DELIMITER STRING      -- e.g. '\x1F'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark.functions import (
    col, split, size, replace, lit, current_timestamp,
    substring, array_construct
)
from snowflake.snowpark.types import StringType


def run(
    session,
    STAGE_PATH,
    HEADER_LIST,
    TARGET_TABLE,
    FILE_TYPE,
    FIXED_WIDTHS,
    ROW_DELIMITER,
    LEGACY_DELIMITER,
    SAFE_DELIMITER
):
    reject_table = f"{TARGET_TABLE}_REJECT"
    expected_cols = len(HEADER_LIST)

    try:
        # --------------------------------------------------
        # 1. Read raw rows
        # --------------------------------------------------
        raw_df = (
            session.read
                .option("RECORD_DELIMITER", ROW_DELIMITER)
                .csv(STAGE_PATH)
        )

        total_rows = raw_df.count()

        # --------------------------------------------------
        # 2. Parse rows
        # --------------------------------------------------
        if FILE_TYPE == "DELIMITED":
            parsed_df = raw_df.select(
                split(
                    replace(col("$1"), LEGACY_DELIMITER, SAFE_DELIMITER),
                    SAFE_DELIMITER
                ).alias("cols"),
                col("$1").alias("raw_row")
            )

        elif FILE_TYPE == "FIXED":
            if FIXED_WIDTHS is None or len(FIXED_WIDTHS) != expected_cols:
                raise ValueError(
                    "FIXED_WIDTHS must be provided and match HEADER_LIST length"
                )

            pos = 1
            col_exprs = []

            for width in FIXED_WIDTHS:
                col_exprs.append(substring(col("$1"), pos, int(width)))
                pos += int(width)

            parsed_df = raw_df.select(
                array_construct(*col_exprs).alias("cols"),
                col("$1").alias("raw_row")
            )

        else:
            raise ValueError("FILE_TYPE must be 'DELIMITED' or 'FIXED'")

        # --------------------------------------------------
        # 3. Column-count validation
        # --------------------------------------------------
        valid_df = parsed_df.filter(size(col("cols")) == expected_cols)
        reject_df = parsed_df.filter(size(col("cols")) != expected_cols)

        # --------------------------------------------------
        # 4. Project valid rows
        # --------------------------------------------------
        valid_out = valid_df.select(
            *[
                col("cols")[i].cast(StringType()).alias(HEADER_LIST[i])
                for i in range(expected_cols)
            ]
        )

        # --------------------------------------------------
        # 5. Replace target table
        # --------------------------------------------------
        valid_out.write.mode("overwrite").save_as_table(TARGET_TABLE)

        # --------------------------------------------------
        # 6. Replace reject table
        # --------------------------------------------------
        reject_out = reject_df.select(
            col("raw_row").cast(StringType()).alias("raw_row"),
            size(col("cols")).cast(StringType()).alias("actual_column_count"),
            lit(str(expected_cols)).alias("expected_column_count"),
            lit("COLUMN_COUNT_MISMATCH").alias("reject_reason"),
            current_timestamp().alias("reject_ts")
        )

        reject_out.write.mode("overwrite").save_as_table(reject_table)

        status = "SUCCESS" if reject_df.count() == 0 else "PARTIAL_SUCCESS"

        telemetry = (
            f"STATUS={status}; "
            f"TOTAL_ROWS={total_rows}; "
            f"VALID_ROWS={valid_df.count()}; "
            f"REJECT_ROWS={reject_df.count()}; "
            f"TARGET_TABLE={TARGET_TABLE}"
        )

        return telemetry

    except Exception as e:
        return f"STATUS=ERROR; MESSAGE={str(e)}"
$$;


CALL UTIL.INGEST_FILE_SP(
    '@ext_stage/legacy/sales_legacy.txt',
    ARRAY_CONSTRUCT('id','name','amount','txn_date'),
    'STG.SALES_DATA',
    'DELIMITED',
    NULL,
    '\n',
    '¿',
    '\x1F'
);



CALL UTIL.INGEST_FILE_SP(
    '@ext_stage/current/orders.psv',
    ARRAY_CONSTRUCT('order_id','cust_id','status','order_ts'),
    'STG.ORDERS',
    'DELIMITED',
    NULL,
    '\n',
    '|',
    '\x1F'
);


CALL UTIL.INGEST_FILE_SP(
    '@ext_stage/fixed/customers.dat',
    ARRAY_CONSTRUCT('cust_id','country','postcode'),
    'STG.CUSTOMERS',
    'FIXED',
    ARRAY_CONSTRUCT(10,3,6),
    '\n',
    NULL,
    NULL
);
