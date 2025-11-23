------------------------------------------------------------
-- TASK 1 — Row Count
------------------------------------------------------------
CREATE OR REPLACE TASK DEMO_DB.DEMO_SCHEMA.TASK_CHECK_COUNT
WAREHOUSE = COMPUTE_WH
AS
  CALL DEMO_DB.DEMO_SCHEMA.SP_CHECK_TABLE_NOT_EMPTY($INPUT_JSON);


------------------------------------------------------------
-- TASK 2 — Null Check (only if rowcount > 0)
------------------------------------------------------------
CREATE OR REPLACE TASK DEMO_DB.DEMO_SCHEMA.TASK_CHECK_NULLS
WAREHOUSE = COMPUTE_WH
AFTER TASK_CHECK_COUNT
WHEN (
    EXISTS (
        SELECT 1
        FROM DEMO_DB.DEMO_SCHEMA.TEST_RESULTS
        WHERE test_name = 'Row Count Test'
          AND result_key = 'TOTAL_ROWS'
          AND TRY_TO_NUMBER(result_val) > 0
    )
)
AS
  CALL DEMO_DB.DEMO_SCHEMA.SP_CHECK_NULLS($INPUT_JSON);


------------------------------------------------------------
-- TASK 3 — Show Results Table
------------------------------------------------------------
CREATE OR REPLACE TASK DEMO_DB.DEMO_SCHEMA.TASK_SHOW_RESULTS
WAREHOUSE = COMPUTE_WH
AFTER TASK_CHECK_NULLS
AS
  CALL DEMO_DB.DEMO_SCHEMA.SP_SHOW_RESULTS();
