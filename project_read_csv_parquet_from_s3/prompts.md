# CSV Ingestion Framework – Prompt Pack

This file contains reproducible prompts to generate all Snowflake Snowpark Python procedures
used in the CSV ingestion framework (v1.9.0 architecture).

---
You are a **Snowflake + Snowpark Python expert** designing a **production-grade, enterprise CSV ingestion framework** from scratch.

The framework must be suitable for **regulated environments**, handle **legacy SAP extracts**, and be **auditable, deterministic, and safe**.

---

## 1. Core constraints (must be enforced)

* Files are stored in an **EXTERNAL STAGE** with **READ-ONLY** permissions
* Files are **UTF-8 encoded**
* Files may come from **legacy SAP systems**
* Some SAP files use a **non-ASCII delimiter `¿`**
* Snowflake CSV parsing must be **quote-safe**
* **No Python string splitting** is allowed for CSV parsing
* **Public procedure interface must be stable**
* The solution must avoid **silent data corruption**

---

## 2. High-level architecture (must implement)

Design a **4-layer ingestion architecture**:

```
External Stage (read-only)
        |
        v
RAW_LINES table        (no CSV parsing, 1 line = 1 row)
        |
        v
NORMALIZED VIEW        (fix legacy delimiters, quote-safe)
        |
        v
CSV ingestion framework (generic, RFC-style CSV only)
```

---

## 3. Legacy SAP delimiter handling (mandatory)

Some files use `¿` as a delimiter.

Rules:

* Snowflake CSV parser does **not reliably support non-ASCII delimiters**
* You must **NOT** parse CSV with `¿`
* You must **NOT** modify files in the external stage

### Required solution (Option 1 – normalization layer)

1. Create a **raw landing table**:

   * One column: `RAW_LINE STRING`
   * Stores each physical line as-is from the external stage

2. Create a **normalized VIEW** that:

   * Replaces `¿` with `;`
   * Replaces **only occurrences outside double-quoted fields**
   * Leaves all other delimiters untouched
   * Leaves quoted content untouched
   * Is deterministic and idempotent
   * Passes through files without `¿` unchanged

3. All CSV parsing must occur **after** this normalization

---

## 4. CSV ingestion framework (generic, reusable)

Build the ingestion framework assuming **only valid ASCII-delimited CSV**.

### Public entry point (must not change)

```sql
STG.LOAD_CSV_DYNAMIC_PY(
  stage_path STRING,
  target_table STRING,
  delim_char STRING,
  file_has_headers BOOLEAN,
  headers_json STRING,
  strict_mode BOOLEAN,
  sample_rows INTEGER
)
```

---

## 5. Internal procedure split (mandatory)

Refactor logic into **three internal Snowflake Python procedures**:

### A. `STG.CSV_PROBE_PLAN_PY` (probe & plan only)

Responsibilities:

* Read a small sample of rows
* AUTO-detect delimiter
* Resolve headers:

  1. File headers
  2. headers_json
  3. System headers (COL_1…COL_N)
* Normalize & deduplicate headers
* Validate duplicate headers (fail if strict_mode)
* Decide COPY eligibility (ASCII delimiter only)

Must return a **VARIANT plan object** containing:

* delimiter
* headers
* expected column count
* skip_header
* copy_allowed
* system_headers_generated
* delimiter detection scores (if AUTO)

Must:

* NOT load data
* NOT create tables

---

### B. `STG.CSV_PREPARE_TABLES_PY` (DDL only)

Responsibilities:

* DROP + CREATE target table (always)
* Ensure reject table exists:
  `<target_table>_REJECTS`

  * RAW_ROW VARIANT
  * ERROR_REASON STRING

Must:

* Contain only DDL
* Be idempotent

---

### C. `STG.CSV_LOAD_EXECUTE_PY` (data movement)

Responsibilities:

* Attempt COPY INTO fast-path:

  * FIELD_DELIMITER
  * SKIP_HEADER
  * FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  * ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
* If COPY fails or is unsafe:

  * Use Snowpark `session.read.csv()` (quote-safe)
* Validate column counts vs headers
* Load valid rows
* Capture invalid rows into reject table

Must:

* Never contain SAP-specific logic
* Never parse CSV manually
* Use Snowflake CSV parser only

---

## 6. Telemetry requirements (mandatory)

Return rich telemetry including:

```json
{
  "load_method": "COPY_FAST_PATH | SNOWPARK_FALLBACK",
  "rows_loaded": <int>,
  "delimiter_normalized": true | false,
  "original_delimiter": "¿",
  "effective_delimiter": ";",
  "fallback_parser": "CSV",
  "copy_duration_sec": <float>,
  "fallback_duration_sec": <float>
}
```

---

## 7. Error handling rules

* Fail early on:

  * Duplicate headers (strict_mode)
  * Empty files
* Never silently corrupt data
* Reject malformed rows, do not discard silently
* Preserve raw data for audit

---

## 8. What to generate

Produce:

1. SQL for:

   * RAW_LINES table
   * NORMALIZED VIEW (quote-safe REGEXP_REPLACE)
2. Full Snowflake Python code for:

   * `CSV_PROBE_PLAN_PY`
   * `CSV_PREPARE_TABLES_PY`
   * `CSV_LOAD_EXECUTE_PY`
   * `LOAD_CSV_DYNAMIC_PY` (orchestrator)
3. A simple ASCII architecture diagram
4. Brief explanation of:

   * Why non-ASCII delimiters break Snowflake CSV
   * Why this solution is enterprise-safe

---

## 9. Quality bar

The final solution must be:

* Production-ready
* Auditable
* Deterministic
* Compatible with Snowflake governance
* Safe for SAP legacy data
* Maintainable by future teams
