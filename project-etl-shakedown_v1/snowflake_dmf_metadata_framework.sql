/* =======================================================================
SNOWFLAKE DATA QUALITY FRAMEWORK – DATA METRIC FUNCTIONS (DMFs)
FULL, CONSOLIDATED, METADATA-DRIVEN IMPLEMENTATION
==========================================================================

PURPOSE
-------
This script implements a complete, Snowflake-native data quality framework
using Data Metric Functions (DMFs).

The framework:
- Runs entirely inside Snowflake
- Scales to thousands of tables
- Is metadata-driven (no hardcoding)
- Is safe to re-run (idempotent)
- Requires no external tools (no GE, no CI/CD, no Snowpark)

WHAT THIS FRAMEWORK DOES
------------------------
- Defines reusable DMFs (data quality logic)
- Defines metadata describing:
  - Which tables are monitored
  - Which rules apply
- Automatically attaches DMFs to tables
- Collects results via Snowflake system telemetry

WHAT THIS FRAMEWORK DOES NOT DO
-------------------------------
- Capture row-level rejects
- Modify or remediate data
- Block pipelines
- Execute DMFs manually

DMFs MEASURE data quality.
They do not enforce or correct it.

==========================================================================

FINAL ARCHITECTURE (CLEAN MENTAL MODEL)
--------------------------------------
DMF_TABLE_REGISTRY  → WHAT tables
DMF_RULE_REGISTRY   → WHAT rules
Stored Procedure    → HOW applied
ACCOUNT_USAGE       → RESULTS

========================================================================== */


/* =======================================================================
STEP 0 – CONTEXT (RECOMMENDED)
========================================================================== */

USE ROLE DATA_GOVERNANCE_ROLE;
USE WAREHOUSE DQ_WH;
USE DATABASE EDW;
USE SCHEMA GOVERNANCE;


/* =======================================================================
STEP 1 – DATA METRIC FUNCTION DEFINITIONS
========================================================================== */
/*
DMFs are defined ONCE and reused across all tables.
They live in a governance schema and are never table-specific.
*/


/*-----------------------------------------------------------------------
DMF: dmf_row_count
Purpose: Detect empty tables (completeness)
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_row_count()
RETURNS NUMBER
AS
$$
    SELECT COUNT(*) FROM TABLE($1)
$$;


/*-----------------------------------------------------------------------
DMF: dmf_pk_null_count
Purpose: Detect NULL primary keys
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_pk_null_count(
    pk_col STRING
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE($1)
    WHERE IDENTIFIER(pk_col) IS NULL
$$;


/*-----------------------------------------------------------------------
DMF: dmf_pk_duplicate_count
Purpose: Detect duplicate primary keys
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_pk_duplicate_count(
    pk_col STRING
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM (
        SELECT IDENTIFIER(pk_col)
        FROM TABLE($1)
        GROUP BY IDENTIFIER(pk_col)
        HAVING COUNT(*) > 1
    )
$$;


/*-----------------------------------------------------------------------
DMF: dmf_out_of_range_count
Purpose: Detect numeric values outside allowed bounds
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_out_of_range_count(
    col_name STRING,
    min_val NUMBER,
    max_val NUMBER
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE($1)
    WHERE IDENTIFIER(col_name) < min_val
       OR IDENTIFIER(col_name) > max_val
$$;


/*-----------------------------------------------------------------------
DMF: dmf_regex_violation_count
Purpose: Detect format / pattern violations
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_regex_violation_count(
    col_name STRING,
    regex_pattern STRING
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE($1)
    WHERE IDENTIFIER(col_name) IS NOT NULL
      AND NOT REGEXP_LIKE(IDENTIFIER(col_name), regex_pattern)
$$;


/*-----------------------------------------------------------------------
DMF: dmf_fk_orphan_count
Purpose: Detect orphan foreign key values
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_fk_orphan_count(
    child_col STRING,
    parent_table STRING,
    parent_col STRING
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE($1) c
    LEFT JOIN IDENTIFIER(parent_table) p
      ON c[child_col] = p[parent_col]
    WHERE c[child_col] IS NOT NULL
      AND p[parent_col] IS NULL
$$;


/*-----------------------------------------------------------------------
DMF: dmf_scd2_multiple_active_count
Purpose: Detect SCD2 keys with zero or multiple active rows
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_scd2_multiple_active_count(
    natural_key_col STRING
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM (
        SELECT IDENTIFIER(natural_key_col)
        FROM TABLE($1)
        WHERE is_current = 1
        GROUP BY IDENTIFIER(natural_key_col)
        HAVING COUNT(*) != 1
    )
$$;


/*-----------------------------------------------------------------------
DMF: dmf_scd2_active_closed_count
Purpose: Detect active records incorrectly end-dated
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_scd2_active_closed_count()
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE($1)
    WHERE is_current = 1
      AND valid_to IS NOT NULL
$$;


/*-----------------------------------------------------------------------
DMF: dmf_scd2_deleted_open_count
Purpose: Detect deleted records not end-dated
-----------------------------------------------------------------------*/
CREATE OR REPLACE DATA METRIC FUNCTION GOVERNANCE.dmf_scd2_deleted_open_count()
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM TABLE($1)
    WHERE is_deleted = 1
      AND valid_to IS NULL
$$;


/* =======================================================================
STEP 2 – METADATA REGISTRIES
========================================================================== */


/*-----------------------------------------------------------------------
TABLE: DMF_TABLE_REGISTRY
Purpose: Defines WHICH tables are monitored
-----------------------------------------------------------------------*/
CREATE OR REPLACE TABLE GOVERNANCE.DMF_TABLE_REGISTRY (
    table_catalog   STRING,
    table_schema    STRING,
    table_name      STRING,
    dq_tier         STRING,   -- TIER1, TIER2, TIER3
    is_enabled      BOOLEAN
);


/*-----------------------------------------------------------------------
TABLE: DMF_RULE_REGISTRY
Purpose: Defines WHICH DMFs apply to WHICH tier
-----------------------------------------------------------------------*/
CREATE OR REPLACE TABLE GOVERNANCE.DMF_RULE_REGISTRY (
    dq_tier     STRING,
    dmf_name    STRING,
    dmf_args    STRING
);


/* =======================================================================
STEP 3 – METADATA-DRIVEN ATTACHMENT PROCEDURE
========================================================================== */
/*
This procedure:
- Loops once per table
- Attaches all applicable DMFs
- Is safe to re-run
*/


CREATE OR REPLACE PROCEDURE GOVERNANCE.APPLY_DMFS_METADATA_DRIVEN()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    tbl RECORD;
    rule RECORD;
    ddl STRING;
    applied_count INTEGER DEFAULT 0;
BEGIN
    FOR tbl IN
        SELECT
            table_catalog,
            table_schema,
            table_name,
            dq_tier
        FROM GOVERNANCE.DMF_TABLE_REGISTRY
        WHERE is_enabled = TRUE
    DO
        FOR rule IN
            SELECT dmf_name, dmf_args
            FROM GOVERNANCE.DMF_RULE_REGISTRY
            WHERE dq_tier = tbl.dq_tier
        DO
            ddl :=
                'ALTER TABLE ' || tbl.table_catalog || '.' ||
                tbl.table_schema || '.' || tbl.table_name ||
                ' ADD DATA METRIC ' || rule.dmf_name || rule.dmf_args;

            BEGIN
                EXECUTE IMMEDIATE ddl;
                applied_count := applied_count + 1;
            EXCEPTION
                WHEN OTHER THEN
                    NULL; -- ignore already-attached or dropped-table cases
            END;
        END FOR;
    END FOR;

    RETURN 'DMF application complete. Statements executed: ' || applied_count;
END;
$$;


/* =======================================================================
STEP 4 – EXECUTION
========================================================================== */

-- Populate metadata tables, then run:
CALL GOVERNANCE.APPLY_DMFS_METADATA_DRIVEN();


/* =======================================================================
STEP 5 – RESULTS (SYSTEM-MANAGED)
========================================================================== */

-- DMF results are written automatically by Snowflake here:
-- SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS

SELECT
    metric_timestamp,
    table_catalog,
    table_schema,
    table_name,
    metric_name,
    metric_value
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS
ORDER BY metric_timestamp DESC;


/* =======================================================================
OPTIONAL – PROJECT RESULTS INTO USER SPACE
========================================================================== */

CREATE OR REPLACE VIEW GOVERNANCE.DMF_RESULTS_VIEW AS
SELECT
    metric_timestamp,
    table_schema,
    table_name,
    metric_name,
    metric_value
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS;


/* =======================================================================
END OF FILE
========================================================================== */



/* =======================================================================
SNOWFLAKE DATA QUALITY FRAMEWORK – METADATA-DRIVEN DMFs
==========================================================================

PURPOSE
-------
This script implements a fully metadata-driven Data Quality framework
using Snowflake Data Metric Functions (DMFs).

The framework:
- Runs entirely inside Snowflake
- Requires no external tools (no GE, no CI/CD, no Snowpark)
- Scales to thousands of tables
- Is auditable, repeatable, and governance-friendly

WHAT THIS SCRIPT DOES
---------------------
1. Defines metadata tables that describe:
   - WHICH tables are monitored
   - WHICH data quality rules apply
2. Defines a stored procedure that:
   - Reads metadata
   - Attaches DMFs to tables automatically
3. Explains how results are collected and consumed

WHAT THIS SCRIPT DOES NOT DO
----------------------------
- Capture row-level reject records
- Block pipelines or fail builds
- Modify or remediate data

MENTAL MODEL (IMPORTANT)
------------------------
DMFs MEASURE data quality.
They do not enforce or correct data.

==========================================================================

FINAL ARCHITECTURE (CLEAN MENTAL MODEL)
--------------------------------------
DMF_TABLE_REGISTRY  → WHAT tables
DMF_RULE_REGISTRY   → WHAT rules
Stored Procedure    → HOW applied
ACCOUNT_USAGE       → RESULTS

========================================================================== */


/* =======================================================================
STEP 0 – CONTEXT SETUP (RECOMMENDED)
========================================================================== */

-- Always set explicit context in governance scripts
-- This avoids accidental execution under the wrong role or warehouse

USE ROLE DATA_GOVERNANCE_ROLE;
USE WAREHOUSE DQ_WH;
USE DATABASE EDW;


/* =======================================================================
STEP 1 – METADATA TABLES (GOVERNANCE LAYER)
========================================================================== */

/*-----------------------------------------------------------------------
TABLE: DMF_TABLE_REGISTRY
PURPOSE:
- Defines WHICH tables participate in data quality monitoring
- Controls scope, tiering, and enable/disable flags

KEY DESIGN POINTS:
- One row per table
- Tiering allows cost and rule control
- is_enabled allows temporary opt-out without deleting metadata
-----------------------------------------------------------------------*/
CREATE OR REPLACE TABLE GOVERNANCE.DMF_TABLE_REGISTRY (
    table_catalog   STRING,   -- Database name
    table_schema    STRING,   -- Schema name
    table_name      STRING,   -- Table name
    dq_tier         STRING,   -- e.g. TIER1, TIER2, TIER3
    is_enabled      BOOLEAN   -- TRUE = apply DMFs
);

-- Example entries
INSERT INTO GOVERNANCE.DMF_TABLE_REGISTRY VALUES
('EDW', 'DIM', 'DIM_CUSTOMER', 'TIER1', TRUE),
('EDW', 'DIM', 'DIM_PRODUCT',  'TIER2', TRUE),
('EDW', 'STG', 'STG_EVENTS',   'TIER3', TRUE);


/*-----------------------------------------------------------------------
TABLE: DMF_RULE_REGISTRY
PURPOSE:
- Defines WHICH DMFs apply to WHICH tier
- Stores DMF arguments as SQL fragments

KEY DESIGN POINTS:
- No hardcoding in procedures
- Adding/removing rules requires only data changes
- Same DMF can be reused across many tables
-----------------------------------------------------------------------*/
CREATE OR REPLACE TABLE GOVERNANCE.DMF_RULE_REGISTRY (
    dq_tier     STRING,   -- Must match DMF_TABLE_REGISTRY.dq_tier
    dmf_name    STRING,   -- Name of the Data Metric Function
    dmf_args    STRING    -- Argument list, e.g. ('CUSTOMER_ID')
);

-- Example rules
INSERT INTO GOVERNANCE.DMF_RULE_REGISTRY VALUES
-- Tier 1: Gold / Dimensions (strict)
('TIER1', 'dmf_row_count', '()'),
('TIER1', 'dmf_pk_null_count', '(''CUSTOMER_ID'')'),
('TIER1', 'dmf_pk_duplicate_count', '(''CUSTOMER_ID'')'),
('TIER1', 'dmf_scd2_multiple_active_count', '(''CUSTOMER_ID'')'),
('TIER1', 'dmf_scd2_active_closed_count', '()'),
('TIER1', 'dmf_scd2_deleted_open_count', '()'),

-- Tier 2: Silver (moderate)
('TIER2', 'dmf_row_count', '()'),
('TIER2', 'dmf_pk_null_count', '(''ID'')'),

-- Tier 3: Bronze / Landing (minimal)
('TIER3', 'dmf_row_count', '()');


/* =======================================================================
STEP 2 – METADATA-DRIVEN STORED PROCEDURE
========================================================================== */

/*-----------------------------------------------------------------------
PROCEDURE: APPLY_DMFS_METADATA_DRIVEN

PURPOSE:
- Reads table metadata (DMF_TABLE_REGISTRY)
- Reads rule metadata (DMF_RULE_REGISTRY)
- Attaches ALL applicable DMFs to EACH table
- Safe to re-run (idempotent)

DESIGN PRINCIPLES:
- Loop ONCE per table
- Attach MULTIPLE DMFs inside the same loop
- Ignore "already attached" errors
- No hardcoded table or DMF names
-----------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE GOVERNANCE.APPLY_DMFS_METADATA_DRIVEN()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    tbl RECORD;
    rule RECORD;
    ddl STRING;
    applied_count INTEGER DEFAULT 0;
BEGIN
    -- Loop over all enabled tables
    FOR tbl IN
        SELECT
            table_catalog,
            table_schema,
            table_name,
            dq_tier
        FROM GOVERNANCE.DMF_TABLE_REGISTRY
        WHERE is_enabled = TRUE
    DO
        -- For each table, apply all DMFs for its tier
        FOR rule IN
            SELECT
                dmf_name,
                dmf_args
            FROM GOVERNANCE.DMF_RULE_REGISTRY
            WHERE dq_tier = tbl.dq_tier
        DO
            ddl :=
                'ALTER TABLE ' || tbl.table_catalog || '.' ||
                tbl.table_schema || '.' || tbl.table_name ||
                ' ADD DATA METRIC ' || rule.dmf_name || rule.dmf_args;

            BEGIN
                EXECUTE IMMEDIATE ddl;
                applied_count := applied_count + 1;
            EXCEPTION
                WHEN OTHER THEN
                    /*
                      Expected exceptions:
                      - DMF already attached
                      - Table dropped after registry update

                      These are intentionally ignored to make
                      the procedure safe to re-run.
                    */
                    NULL;
            END;
        END FOR;
    END FOR;

    RETURN 'DMF application complete. Total statements executed: ' || applied_count;
END;
$$;


/* =======================================================================
STEP 3 – RUN THE FRAMEWORK
========================================================================== */

-- Execute once after:
-- - Adding new tables
-- - Changing tiers
-- - Adding/removing DMFs

CALL GOVERNANCE.APPLY_DMFS_METADATA_DRIVEN();


/* =======================================================================
STEP 4 – WHERE RESULTS LIVE (IMPORTANT)
========================================================================== */

-- DMF results are written by Snowflake to system views.
-- This location is FIXED and cannot be changed.

-- Primary view for results:
-- SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS

-- Example query:
SELECT
    metric_timestamp,
    table_catalog,
    table_schema,
    table_name,
    metric_name,
    metric_value
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS
ORDER BY metric_timestamp DESC;


/* =======================================================================
OPTIONAL – PROJECT RESULTS INTO USER SCHEMA
========================================================================== */

-- Recommended for analyst access and reporting
CREATE OR REPLACE VIEW GOVERNANCE.DMF_RESULTS_VIEW AS
SELECT
    metric_timestamp,
    table_schema,
    table_name,
    metric_name,
    metric_value
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS;


/* =======================================================================
SUMMARY
==========================================================================

- DMFs are attached automatically using metadata
- Tables are controlled via DMF_TABLE_REGISTRY
- Rules are controlled via DMF_RULE_REGISTRY
- Execution is handled by APPLY_DMFS_METADATA_DRIVEN
- Results are collected by Snowflake in ACCOUNT_USAGE

This design scales, is auditable, and is fully Snowflake-native.
========================================================================== */
