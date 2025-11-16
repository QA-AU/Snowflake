README.md — Shakedown Test Framework for Snowflake

1. Overview
The Shakedown Test Framework is a modular, metadata-driven Python engine designed to run automated structural, quality, business, SCD2, duplicate, and relational integrity tests against Snowflake tables.
It is fully compatible with:


Snowflake Python Worksheets

Snowpark Python (Session)

JSON metadata stored in an accessible Snowflake Stage (default: @temp_config_stage)

The engine supports:

23 built-in tests

Easy extension via additional test modules

Automatic test discovery

Automatic metadata discovery

Automatic multi-table execution

Unified result logging into STG.QA_SHAKEDOWN_RESULTS


2. Folder Structure
engine/
    helpers.py
    registry.py
    orchestrator.py
    tests_structural.py
    tests_scd2_temporal.py
    tests_business.py
    tests_duplicates.py
    tests_quality.py
    tests_relational.py

metadata_loader.py
auto_runner.py
main_caller.py
README.md


3. Installation
Upload your engine files into a Snowflake Python Worksheet environment:

Create a local ZIP file of this repo.

Upload it into your Python Worksheet "imports" section.

Create a temporary stage for metadata JSON:

CREATE OR REPLACE STAGE temp_config_stage;

Upload your metadata JSON files into that stage:


PUT file://my_table.json @temp_config_stage AUTO_COMPRESS=FALSE;
PUT file://ORDER_DIM.json @temp_config_stage;


4. Metadata JSON Format
Each table requires one JSON file stored in @temp_config_stage.
Example metadata file:
{
  "version": "1.0",
  "table_fqn": "CORE.SALES.ORDER_DIM",

  "table": {
    "bk_columns": ["ORDER_ID"],
    "pk_columns": ["SK_ORDER_ID"],
    "scd2_required_columns": ["start_dt", "end_dt", "is_active"],

    "business_date_column": "business_date",
    "scd2_change_columns": ["status", "amount"],

    "etl_batch_columns": ["batch_id", "load_ts"],

    "allowed_values": {
      "status": ["OPEN", "CLOSED", null]
    },

    "whitespace_columns": ["customer_name"],
    "numeric_ranges": {
      "amount": {"min": 0, "max": 999999}
    },

    "fk_relations": [
      {
        "child_column": "cust_id",
        "parent_table": "CORE.CUSTOMER_DIM",
        "parent_column": "cust_sk"
      }
    ],

    "orphan_checks": [
      {
        "fact_table": "CORE.SALES_FACT",
        "fact_column": "product_id",
        "dim_table": "CORE.PRODUCT_DIM",
        "dim_column": "product_id"
      }
    ]
  },

  "tests_to_run": [
    "BK_NOT_NULL",
    "PK_NOT_NULL",
    "SCD2_COLS_NOT_NULL",
    "NO_FUTURE_DATES",
    "INSERT_COUNT",
    "DUPLICATE_ROWS_BK",
    "COL_ALLOWED_VALUES",
    "FK_RELATION_CHECK"
  ]
}


5. Running Tests
You will call everything through main_caller.py.
5.1. Import main_caller
from main_caller import main_caller


5.2. Run tests for a single table
df = main_caller(
    session,
    mode="single",
    business_date="2025-02-01",
    table_fqn="CORE.SALES.ORDER_DIM",
    debug=True
)

df.show()


5.3. Auto-run ALL tables discovered from metadata JSON files
results = main_caller(
    session,
    mode="auto",
    business_date="2025-02-01",
    debug=True
)

for table, df in results.items():
    print("=== RESULTS FOR:", table)
    df.show()


5.4. Auto-run with filter
Run only tables with names matching "ORDER" or "CUSTOMER":
results = main_caller(
    session,
    mode="auto",
    business_date="2025-02-01",
    tables_filter=["ORDER", "CUSTOMER"],
    debug=True
)


6. Result Storage
All test results are written to:
STG.QA_SHAKEDOWN_RESULTS

Schema:
ColumnDescriptionRUN_NAMEUnique name of test runTABLE_FQNTable under testTEST_NAMEName of executed testSQL_USEDSQL executedPASS_FLAGTRUE/FALSEMETRICSJSON dictionary of metricsERRORError message (if any)DURATION_MSExecution time

7. Test Groups
Structural Tests (1–6)


BK_NOT_NULL

PK_NOT_NULL

SCD2_COLS_NOT_NULL

SK_UNIQUENESS

NO_FUTURE_DATES

ETL_BATCH_COLS_NOT_NULL

SCD2 Temporal Tests (7–12)

SCD2_START_DATE_VALID

SCD2_END_DATE_VALID

SCD2_ONLY_ONE_ACTIVE

SCD2_CHANGE_DETECTION

SCD2_EFFECTIVE_SEQUENCE

SCD2_SURROGATE_REUSE

Business / Load Tests (13–16)

BUSINESS_DATE_MAX_MATCH

INSERT_COUNT

UPDATE_COUNT

DELETE_COUNT

Duplicate Tests (17–18)

DUPLICATE_ROWS_BK

DUPLICATE_ROWS_BK_SCD2

Quality Tests (19–21)

COL_ALLOWED_VALUES

COL_WHITESPACE_CLEAN

COL_RANGE_VALIDATION

Relational Tests (22–23)

FK_RELATION_CHECK

ORPHAN_RECORDS_CHECK



8. Debug Mode
Enable debug output:
df = main_caller(
    session,
    mode="single",
    business_date="2025-02-01",
    table_fqn="CORE.SALES.ORDER_DIM",
    debug=True
)

Debug mode prints:

Metadata loading

File discovery

Active tests

SQLs as they execute

Detected errors


9. Troubleshooting
 "Metadata file not found"
Check stage:
LIST @temp_config_stage;

“Test not in registry”
Ensure tests_to_run contains valid test names.

 “Missing required metadata field”
Check metadata JSON validity.

10. Extending the Framework
To add a new test:


Create a new module in engine/tests_new.py


Write test function with signature:


def test_my_new_test(session, meta, run_name):
    ...



Add the function to TEST_REGISTRY in registry.py


Add the test name to tests_to_run inside metadata JSON


11. License
Internal use only (custom development project).

