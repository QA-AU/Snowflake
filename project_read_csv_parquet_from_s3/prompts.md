# CSV Ingestion Framework – Prompt Pack

This file contains reproducible prompts to generate all Snowflake Snowpark Python procedures
used in the CSV ingestion framework (v1.9.0 architecture).

---

## Prompt 1 — CSV Probe & Plan

You are a Snowflake + Snowpark Python expert.

Create a Snowflake Python stored procedure named:

STG.CSV_PROBE_PLAN_PY

Purpose:
- Read a small sample of rows from a staged CSV file
- Detect delimiter (AUTO mode supported)
- Resolve headers using the following priority:
  1) From file if file_has_headers = TRUE
  2) From headers_json if provided
  3) Otherwise generate system headers COL_1..COL_N
- Normalize headers:
  - Uppercase
  - ASCII only
  - Replace non-alphanumeric with _
  - Deduplicate deterministically (COL, COL_2, COL_3)
- Detect duplicate headers and optionally fail if strict_mode = TRUE
- Decide whether COPY fast-path is allowed (ASCII delimiter only)

Inputs:
- stage_path STRING
- delim_char STRING (or AUTO)
- file_has_headers BOOLEAN
- headers_json STRING
- strict_mode BOOLEAN
- sample_rows INTEGER

Output:
Return a VARIANT plan object with:
- status (OK or FAIL)
- delimiter
- delimiter_scores (if AUTO)
- headers (array)
- expected_cols (integer)
- copy_allowed (boolean)
- skip_header (0 or 1)
- system_headers_generated (boolean)

Constraints:
- Do not load data
- Do not create tables
- Sampling only (LIMIT sample_rows)
- Use Snowflake SQL to read from the stage
- Snowflake-safe, production-grade code

---

## Prompt 2 — Prepare Tables (DDL only)

You are a Snowflake Python stored procedure expert.

Create a Snowflake Python stored procedure named:

STG.CSV_PREPARE_TABLES_PY

Purpose:
- Perform all DDL required before loading
- DROP + CREATE the target table every time
- Ensure a reject table exists

Inputs:
- target_table STRING
- headers ARRAY

Behavior:
- DROP TABLE IF EXISTS target_table
- CREATE TABLE target_table with all columns as STRING
- CREATE TABLE IF NOT EXISTS <target_table>_REJECTS with columns:
  - RAW_ROW VARIANT
  - ERROR_REASON STRING

Constraints:
- No file reads
- No COPY
- No Snowpark DataFrame reads
- DDL only
- Idempotent and safe to rerun

Return:
- A simple STRING status like "TABLES_PREPARED"

---

## Prompt 3 — Load & Validate (COPY + fallback)

You are a Snowflake Snowpark Python expert.

Create a Snowflake Python stored procedure named:

STG.CSV_LOAD_EXECUTE_PY

Purpose:
- Load CSV data using a two-path strategy:
  1) COPY INTO fast-path when allowed
  2) Snowpark fallback using Snowflake CSV parser when COPY fails or is unsafe
- Validate column counts against expected headers
- Capture rejected rows during fallback

Inputs:
- stage_path STRING
- target_table STRING
- plan VARIANT (produced by CSV_PROBE_PLAN_PY)

Behavior:
- If plan.copy_allowed = TRUE:
  - Build COPY INTO SQL using:
    - FIELD_DELIMITER = plan.delimiter
    - SKIP_HEADER = plan.skip_header
    - FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    - ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  - Attempt COPY
  - If COPY succeeds → return rows_loaded + telemetry
  - If COPY fails → continue to fallback
- Fallback:
  - Use session.read.csv() with Snowflake CSV parser
  - Handle quoted fields correctly
  - Validate array_size(value) == plan.expected_cols
  - Load valid rows into target_table
  - Write invalid rows into <target_table>_REJECTS
- Return:
  - load_method (COPY_FAST_PATH or SNOWPARK_FALLBACK)
  - rows_loaded
  - telemetry including:
    - fallback_parser = "CSV"
    - copy_error (if any)
    - timing metrics

Constraints:
- No DDL
- No header detection
- Use QUOTE_CHAR = '"'
- Production-safe Snowflake code

---

## Prompt 4 — Public Orchestrator (interface unchanged)

You are a Snowflake Python orchestration expert.

Create a Snowflake Python stored procedure named:

STG.LOAD_CSV_DYNAMIC_PY

This is the PUBLIC entry point.
The interface must remain unchanged.

Inputs:
- stage_path STRING
- target_table STRING
- delim_char STRING
- file_has_headers BOOLEAN
- headers_json STRING
- strict_mode BOOLEAN
- sample_rows INTEGER

Behavior:
1) Call STG.CSV_PROBE_PLAN_PY to generate a load plan
2) If plan.status != "OK", return plan immediately
3) Call STG.CSV_PREPARE_TABLES_PY to DROP + CREATE tables
4) Call STG.CSV_LOAD_EXECUTE_PY to perform the load
5) Return a single VARIANT containing:
   - status = SUCCESS
   - plan
   - load_method
   - rows_loaded
   - telemetry

Constraints:
- No file parsing logic
- No COPY logic
- No DDL logic
- Acts only as an orchestrator
- Snowflake production-quality code
