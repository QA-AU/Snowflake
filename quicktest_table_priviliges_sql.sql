quicktest_table_priviliges_sql.py

-- ======================================================
-- Step 0: Set variables
-- ======================================================
SET TARGET_DB     = 'MY_DATABASE';
SET TARGET_SCHEMA = 'MY_SCHEMA';
SET TARGET_USER   = 'USERNAME_TO_CHECK';   -- e.g. 'ANALYST1'

-- ======================================================
-- Step 1: Collect all tables in the schema
-- ======================================================
CREATE OR REPLACE TEMP TABLE QA_TMP_TABLES AS
SELECT TABLE_CATALOG,
       TABLE_SCHEMA,
       TABLE_NAME
FROM IDENTIFIER($TARGET_DB || '.INFORMATION_SCHEMA.TABLES')
WHERE TABLE_SCHEMA = $TARGET_SCHEMA
  AND TABLE_TYPE   = 'BASE TABLE';

-- ======================================================
-- Step 2: Collect object privileges from INFORMATION_SCHEMA
-- ======================================================
CREATE OR REPLACE TEMP TABLE QA_TMP_GRANTS AS
SELECT GRANTEE,
       PRIVILEGE,
       OBJECT_CATALOG,
       OBJECT_SCHEMA,
       OBJECT_NAME
FROM IDENTIFIER($TARGET_DB || '.INFORMATION_SCHEMA.OBJECT_PRIVILEGES')
WHERE OBJECT_SCHEMA = $TARGET_SCHEMA
  AND OBJECT_TYPE   = 'TABLE'
  AND GRANTEE       = $TARGET_USER;

-- ======================================================
-- Step 3: Join tables with grants
-- ======================================================
CREATE OR REPLACE TEMP TABLE QA_TMP_ACCESS_REPORT AS
SELECT t.TABLE_CATALOG,
       t.TABLE_SCHEMA,
       t.TABLE_NAME,
       COALESCE(LISTAGG(g.PRIVILEGE, ', ') WITHIN GROUP (ORDER BY g.PRIVILEGE), 'NO ACCESS') AS USER_PRIVILEGES
FROM QA_TMP_TABLES t
LEFT JOIN QA_TMP_GRANTS g
  ON t.TABLE_CATALOG = g.OBJECT_CATALOG
 AND t.TABLE_SCHEMA  = g.OBJECT_SCHEMA
 AND t.TABLE_NAME    = g.OBJECT_NAME
GROUP BY t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME;

-- ======================================================
-- Step 4: View results
-- ======================================================
SELECT *
FROM QA_TMP_ACCESS_REPORT
ORDER BY TABLE_NAME;

-- ======================================================
-- Step 5: Cleanup
-- ======================================================
DROP TABLE IF EXISTS QA_TMP_ACCESS_REPORT;
DROP TABLE IF EXISTS QA_TMP_GRANTS;
DROP TABLE IF EXISTS QA_TMP_TABLES;
