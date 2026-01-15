You are a Snowflake data-engineering expert.

I already have an existing CSV ingestion framework in Snowflake using Snowpark Python with the following characteristics:

External stage is READ-ONLY

Public entry procedure: STG.LOAD_CSV_DYNAMIC_PY (interface must remain unchanged)

Internal procedures:

STG.CSV_PROBE_PLAN_PY (probe & plan)

STG.CSV_PREPARE_TABLES_PY (DDL only)

STG.CSV_LOAD_EXECUTE_PY (COPY fast-path + Snowpark fallback)

The framework assumes RFC-style CSV:

ASCII delimiter

Double-quote enclosure

COPY INTO preferred

AUTO delimiter detection exists

Non-ASCII delimiters (e.g. ¿ from legacy SAP systems) currently break parsing

External stage cannot be modified (read-only)

I want you to extend the existing framework to safely support legacy SAP files that use the inverted question mark ¿ as a delimiter, while keeping the framework generic.

Required design (must follow exactly)

Do NOT modify the external stage files

Introduce a new raw landing table that stores each physical line as STRING

Introduce a normalized VIEW on top of the raw table that:

Replaces ¿ with a valid ASCII delimiter (use ;)

Replaces ONLY occurrences of ¿ that are OUTSIDE double-quoted fields

Leaves all other delimiters (, | ; \t) untouched

Leaves quoted content untouched

The normalization must be:

Quote-aware

Deterministic

Idempotent

Files that do NOT contain ¿ must pass through unchanged

After normalization:

The existing AUTO delimiter detection must still work

COPY fast-path must still work

Snowpark fallback must still work

The normalization layer must be transparent to callers:

No change to STG.LOAD_CSV_DYNAMIC_PY signature

No SAP-specific logic in COPY or fallback

Add telemetry fields:

"delimiter_normalized": true|false

"original_delimiter": "¿" (when applicable)

"effective_delimiter": ";" (when applicable)

What to generate

Produce:

SQL to create the RAW_LINES landing table

SQL to create the NORMALIZED view (quote-safe REGEXP_REPLACE)

Minimal changes needed in STG.CSV_PROBE_PLAN_PY to read from the view instead of the stage

Confirmation that STG.CSV_LOAD_EXECUTE_PY remains unchanged

A short explanation of why this approach is safe and enterprise-grade

Constraints:

Do not use Python string splitting for CSV parsing

Do not attempt COPY or Snowpark CSV parsing with ¿

Use Snowflake-native capabilities

Assume UTF-8 files from legacy SAP systems

Keep the solution production-ready and auditable