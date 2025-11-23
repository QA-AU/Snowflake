------------------------------------------------------------
-- 2. DROP TASKS
------------------------------------------------------------
DROP TASK IF EXISTS DEMO_DB.DEMO_SCHEMA.TASK_SHOW_RESULTS;
DROP TASK IF EXISTS DEMO_DB.DEMO_SCHEMA.TASK_CHECK_NULLS;
DROP TASK IF EXISTS DEMO_DB.DEMO_SCHEMA.TASK_CHECK_COUNT;


------------------------------------------------------------
-- 3. DROP STORED PROCEDURES
------------------------------------------------------------
DROP PROCEDURE IF EXISTS DEMO_DB.DEMO_SCHEMA.SP_CHECK_TABLE_NOT_EMPTY(STRING);
DROP PROCEDURE IF EXISTS DEMO_DB.DEMO_SCHEMA.SP_CHECK_NULLS(STRING);
DROP PROCEDURE IF EXISTS DEMO_DB.DEMO_SCHEMA.SP_SHOW_RESULTS();


------------------------------------------------------------
-- 4. DROP TEST RESULTS TABLE
------------------------------------------------------------
DROP TABLE IF EXISTS DEMO_DB.DEMO_SCHEMA.TEST_RESULTS;


------------------------------------------------------------
-- 5. OPTIONAL: UNSET THE SESSION VARIABLE
------------------------------------------------------------
UNSET INPUT_JSON;