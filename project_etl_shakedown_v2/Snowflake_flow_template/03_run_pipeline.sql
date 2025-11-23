------------------------------------------------------------
-- ENABLE ALL TASKS
------------------------------------------------------------
ALTER TASK DEMO_DB.DEMO_SCHEMA.TASK_CHECK_COUNT RESUME;
ALTER TASK DEMO_DB.DEMO_SCHEMA.TASK_CHECK_NULLS RESUME;
ALTER TASK DEMO_DB.DEMO_SCHEMA.TASK_SHOW_RESULTS RESUME;


------------------------------------------------------------
-- RUN THE PIPELINE — ONLY RUN TASK 1
------------------------------------------------------------
EXECUTE TASK DEMO_DB.DEMO_SCHEMA.TASK_CHECK_COUNT;


------------------------------------------------------------
-- VIEW FINAL RESULTS AS A TABLE
------------------------------------------------------------
SELECT * FROM TABLE(DEMO_DB.DEMO_SCHEMA.SP_SHOW_RESULTS());
