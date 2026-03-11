"""
scd2_validator_3tbl_11_Mar_26.py
=================================
SCD2 post-load validation tool — 3-Table input edition — Snowflake Python Worksheet.

HOW TO USE:
  1. Fill in the CONFIG dict below (the only section you need to edit)
  2. Paste this entire file into a Snowflake Python Worksheet
  3. Click Run

NO CREDENTIALS NEEDED:
  Runs inside Snowflake using the active worksheet session.
  Just specify the database and warehouse to use.

WHAT IT DOES:
  Validates that SCD2 was correctly applied to your target table for a given
  business date. Accepts three explicit table inputs:
    1. source       — today's flat source snapshot (no SCD2 columns required)
    2. target_pre   — target table state BEFORE today's SCD2 run (pre-load)
    3. target_post  — target table state AFTER today's SCD2 run (post-load)
  Reads all three tables (never writes to any), simulates the expected SCD2
  output in temp tables, then compares against target_post actuals.
  Results are written to a configurable Snowflake results table.

HOW THIS DIFFERS FROM THE 2-TABLE VERSION (scd2_validator_snowflake_22_Feb_26.py):
  - source table does NOT need STRT_DT / END_DT / DELETED_FLAG columns.
    It can be a plain flat staging/incremental table.
  - target_pre is provided explicitly as a separate table (yesterday's state)
    instead of being derived by filtering the target with a D-1 date range.
  - target_post is the post-SCD2 table used for all validation checks.
  - No date-range filtering on source or target_pre — each table is already
    the right snapshot, making the logic simpler and less error-prone.

7 CHECKS PERFORMED:
  1. INSERT  — new active row written
  2. UPDATE  — new active version written
  3. UPDATE  — old version correctly expired (END_DT = business_date - 1)
  4. DELETE  — delete marker written
  5. DELETE  — old version correctly expired (END_DT = business_date - 1)
  6. NO CHANGE — unchanged rows still active
  7. NO EXTRA rows written to target

NULL KEY HANDLING:
  PKs are not enforced — NULL is treated as a valid key value.
  All key joins use EQUAL_NULL() so NULL = NULL resolves to TRUE.
  Non-match detection uses _EXISTS_FLAG sentinel (not key IS NULL)
  to avoid false INSERT/DELETE classification on NULL-key rows.

RESULTS TABLE:
  Created automatically if it does not exist.
  Results for the same TARGET_POST_TABLE + BUSINESS_DATE are replaced on each run.
"""

# =============================================================================
# !! CONFIGURE HERE — the only section you need to edit !!
# =============================================================================
CONFIG = {
    "connection": {
        "database":  "your_database",   # database containing all three tables
        "warehouse": "your_warehouse",  # warehouse to use for the run
    },
    "run": {
        "business_date": "2026-02-21",  # date being validated (YYYY-MM-DD)
    },
    # Today's flat source snapshot — no SCD2 columns required.
    # Must contain key columns and payload columns only.
    "source": {
        "schema": "SRC_SCHEMA",
        "table":  "SRC_TABLE",
    },
    # Target table state BEFORE today's SCD2 run.
    # Must contain SCD2 control columns (STRT_DT, END_DT, DELETED_FLAG).
    # Active rows = END_DT = '9999-12-31' AND DELETED_FLAG = FALSE.
    "target_pre": {
        "schema": "TGT_SCHEMA",
        "table":  "TGT_TABLE_PRE",
    },
    # Target table state AFTER today's SCD2 run — used for all validation.
    # Must contain SCD2 control columns (STRT_DT, END_DT, DELETED_FLAG).
    "target_post": {
        "schema": "TGT_SCHEMA",
        "table":  "TGT_TABLE_POST",
        # SCD2 control column names — change only if your table uses different names.
        # Defaults: STRT_DT, END_DT, DELETED_FLAG
        # These column names are assumed to be the same in target_pre and target_post.
        "scd2_columns": {
            "strt_dt":      "STRT_DT",
            "end_dt":       "END_DT",
            "deleted_flag": "DELETED_FLAG",
        },
    },
    # Natural / composite key columns.
    # NULL is treated as a valid key value — no PK enforcement assumed.
    "keys": ["KEY_COL1", "KEY_COL2"],
    # Columns to hash for change detection.
    # MUST contain at least one column — empty list disables UPDATE detection.
    # Exclude: key cols, audit cols, SCD2 control cols (STRT_DT, END_DT, etc.)
    # MUST match exactly the columns your ETL pipeline hashes — mismatches
    # cause false FAIL on Check 7 (unexpected rows) or false PASS on Check 2.
    "payload_columns": ["COL_A", "COL_B", "COL_C"],
    # Audit columns — listed here for documentation only (not used in hashing).
    # No RECORD_HASH or SK columns are required in source or target.
    "audit_columns": ["CREATED_AT", "UPDATED_AT", "BATCH_ID"],
    "results": {
        "schema": "AUDIT_SCHEMA",
        "table":  "SCD2_VALIDATION_RESULTS",
    },
}
# =============================================================================
# END OF CONFIG — do not edit below this line
# =============================================================================


import logging
import traceback
from datetime import datetime
import snowflake.snowpark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scd2_validator_3tbl")


# =============================================================================
# CONFIG VALIDATOR
# =============================================================================
def validate_config(cfg: dict):
    errors = []

    for section in (
        "connection", "run", "source", "target_pre", "target_post",
        "keys", "payload_columns", "results",
    ):
        if section not in cfg:
            errors.append(f"Missing required section: '{section}'")

    if "connection" in cfg:
        for field in ("database", "warehouse"):
            if not cfg["connection"].get(field):
                errors.append(f"connection.{field} is required")

    if "run" in cfg and not cfg["run"].get("business_date"):
        errors.append("run.business_date is required (format: YYYY-MM-DD)")

    if "source" in cfg:
        for field in ("schema", "table"):
            if not cfg["source"].get(field):
                errors.append(f"source.{field} is required")

    if "target_pre" in cfg:
        for field in ("schema", "table"):
            if not cfg["target_pre"].get(field):
                errors.append(f"target_pre.{field} is required")

    if "target_post" in cfg:
        for field in ("schema", "table"):
            if not cfg["target_post"].get(field):
                errors.append(f"target_post.{field} is required")

    if "keys" in cfg and not cfg["keys"]:
        errors.append("keys must contain at least one column name")

    if "payload_columns" in cfg and not cfg["payload_columns"]:
        errors.append(
            "payload_columns must contain at least one column name — "
            "empty list disables UPDATE detection entirely"
        )

    if "results" in cfg:
        for field in ("schema", "table"):
            if not cfg["results"].get(field):
                errors.append(f"results.{field} is required")

    if errors:
        for e in errors:
            log.error(f"Config error: {e}")
        raise ValueError(f"Config has {len(errors)} error(s) — see log above")

    # Apply SCD2 column name defaults to target_post
    scd2_defaults = {
        "strt_dt":      "STRT_DT",
        "end_dt":       "END_DT",
        "deleted_flag": "DELETED_FLAG",
    }
    user_scd2 = cfg["target_post"].get("scd2_columns", {})
    cfg["target_post"]["scd2_columns"] = {**scd2_defaults, **user_scd2}

    cfg.setdefault("audit_columns", [])

    log.info("Config validated successfully")
    return cfg


# =============================================================================
# SQL BUILDER
# =============================================================================
class SCD2SqlBuilder:
    """
    Builds all validation SQL from config.
    No hardcoded column names — everything driven by CONFIG.

    3-TABLE APPROACH:
      - tmp_src()      reads source directly — no date filter (flat snapshot)
      - tmp_tgt_prev() reads target_pre directly — filters to open-ended active
                       rows (END_DT = '9999-12-31' AND DELETED_FLAG = FALSE)
      - All validation checks (check_missing, check_extra, check_expiry)
        read from target_post (post-SCD2 table)

    NULL KEY FIXES applied throughout:
      - _key_join()      uses EQUAL_NULL() instead of = (Change 1)
      - tmp_tgt_prev()   adds _EXISTS_FLAG sentinel (Change 2)
      - tmp_src()        adds _EXISTS_FLAG sentinel (Change 2b)
      - tmp_classified() uses _EXISTS_FLAG IS NULL for non-match detection (Change 3)
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.db            = cfg["connection"]["database"]
        self.business_date = cfg["run"]["business_date"]

        self.src_schema = cfg["source"]["schema"]
        self.src_table  = cfg["source"]["table"]
        self.src_fqn    = f"{self.db}.{self.src_schema}.{self.src_table}"

        self.tgt_pre_schema = cfg["target_pre"]["schema"]
        self.tgt_pre_table  = cfg["target_pre"]["table"]
        self.tgt_pre_fqn    = f"{self.db}.{self.tgt_pre_schema}.{self.tgt_pre_table}"

        self.tgt_post_schema = cfg["target_post"]["schema"]
        self.tgt_post_table  = cfg["target_post"]["table"]
        self.tgt_post_fqn    = f"{self.db}.{self.tgt_post_schema}.{self.tgt_post_table}"

        scd2 = cfg["target_post"]["scd2_columns"]
        self.strt_dt      = scd2["strt_dt"]
        self.end_dt       = scd2["end_dt"]
        self.deleted_flag = scd2["deleted_flag"]

        self.keys            = cfg["keys"]
        self.payload_columns = cfg["payload_columns"]
        self.audit_columns   = cfg.get("audit_columns", [])

        self.res_schema = cfg["results"]["schema"]
        self.res_table  = cfg["results"]["table"]
        self.res_fqn    = f"{self.db}.{self.res_schema}.{self.res_table}"

        self._key_cols_raw = ", ".join(self.keys)
        self._payload_cols = ", ".join(self.payload_columns)

    def _hash_expr(self, table_alias: str = None) -> str:
        """SHA2-256 hash over payload columns. COALESCE ensures null safety."""
        prefix = f"{table_alias}." if table_alias else ""
        parts  = [
            f"COALESCE(CAST({prefix}{col} AS VARCHAR), '')"
            for col in self.payload_columns
        ]
        inner = ",\n        ".join(parts)
        return f"SHA2(CONCAT_WS('||',\n        {inner}\n    ), 256)"

    def _key_join(self, left: str, right: str) -> str:
        """
        NULL-safe key join using EQUAL_NULL().
        Standard = returns NULL (not TRUE) for NULL = NULL, which breaks all
        join-based classification when PKs are not enforced and NULL is a
        valid key value. EQUAL_NULL(NULL, NULL) returns TRUE.
        """
        return "\n    AND ".join(
            f"EQUAL_NULL({left}.{k}, {right}.{k})" for k in self.keys
        )

    def _col_list(self, alias: str, cols: list) -> str:
        return ",\n    ".join(f"{alias}.{c}" for c in cols)

    # -------------------------------------------------------------------------
    def tmp_src(self) -> str:
        """
        Reads today's source snapshot directly — no date filter needed.
        Source is a flat table; no SCD2 columns (STRT_DT/END_DT/DELETED_FLAG)
        are required. business_date is stamped as STRT_DT for downstream use.
        Includes _EXISTS_FLAG = 'Y' sentinel for NULL-safe non-match detection.
        """
        key_cols = "\n    ".join(f"{c}," for c in self.keys)
        payload  = "\n    ".join(f"{c}," for c in self.payload_columns)
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_SRC AS
SELECT
    {key_cols}
    {payload}
    {self._hash_expr()} AS _RECORD_HASH,
    '{self.business_date}'::DATE AS {self.strt_dt},
    'Y' AS _EXISTS_FLAG
FROM {self.src_fqn};
""".strip()

    def tmp_tgt_prev(self) -> str:
        """
        Reads the pre-SCD2 target snapshot directly (target_pre table).
        No date-range filtering — the table already represents yesterday's state.
        Filters to open-ended active rows only:
          END_DT = '9999-12-31' AND DELETED_FLAG = FALSE
        This excludes historical expired rows and existing soft-delete markers
        from prior runs, giving a clean set of currently active records.
        Includes _EXISTS_FLAG = 'Y' sentinel for NULL-safe non-match detection.
        """
        key_cols = "\n    ".join(f"T.{c}," for c in self.keys)
        payload  = "\n    ".join(f"T.{c}," for c in self.payload_columns)
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_TGT_PREV AS
SELECT
    {key_cols}
    {payload}
    {self._hash_expr('T')} AS _RECORD_HASH,
    T.{self.strt_dt},
    T.{self.end_dt},
    'Y' AS _EXISTS_FLAG
FROM {self.tgt_pre_fqn} T
WHERE T.{self.end_dt}       = '9999-12-31'
  AND T.{self.deleted_flag} = FALSE;
""".strip()

    def tmp_classified(self) -> str:
        """
        Classifies every key as INSERT / UPDATE / DELETE by comparing
        TMP_SRC against TMP_TGT_PREV.

        Non-match detection uses _EXISTS_FLAG IS NULL (not key IS NULL).
        When a key is NULL and a LEFT JOIN finds no match, the key column
        is NULL in both the source row AND the unmatched join result —
        making key IS NULL ambiguous. _EXISTS_FLAG is a non-nullable literal
        'Y' that is only NULL when the join found no matching row.
        """
        key_s   = "\n    ".join(f"S.{c}," for c in self.keys)
        key_t   = "\n    ".join(f"T.{c}," for c in self.keys)
        pay_s   = "\n    ".join(f"S.{c}," for c in self.payload_columns)
        pay_t   = "\n    ".join(f"T.{c}," for c in self.payload_columns)
        join_st = self._key_join("S", "T")
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_CLASSIFIED AS

-- INSERT: in source today, absent from pre-SCD2 target
SELECT
    {key_s}
    {pay_s}
    S._RECORD_HASH,
    S.{self.strt_dt},
    'INSERT' AS CHANGE_TYPE
FROM TMP_SRC S
LEFT JOIN TMP_TGT_PREV T ON {join_st}
WHERE T._EXISTS_FLAG IS NULL

UNION ALL

-- UPDATE: in both, hash changed
SELECT
    {key_s}
    {pay_s}
    S._RECORD_HASH,
    S.{self.strt_dt},
    'UPDATE' AS CHANGE_TYPE
FROM TMP_SRC S
INNER JOIN TMP_TGT_PREV T ON {join_st}
WHERE S._RECORD_HASH <> T._RECORD_HASH

UNION ALL

-- DELETE: in pre-SCD2 target, absent from source today
SELECT
    {key_t}
    {pay_t}
    T._RECORD_HASH,
    '{self.business_date}'::DATE AS {self.strt_dt},
    'DELETE' AS CHANGE_TYPE
FROM TMP_TGT_PREV T
LEFT JOIN TMP_SRC S ON {self._key_join('T', 'S')}
WHERE S._EXISTS_FLAG IS NULL;
""".strip()

    def tmp_expected(self) -> str:
        key_t   = "\n    ".join(f"T.{c}," for c in self.keys)
        pay_t   = "\n    ".join(f"T.{c}," for c in self.payload_columns)
        key_c   = "\n    ".join(f"C.{c}," for c in self.keys)
        pay_c   = "\n    ".join(f"C.{c}," for c in self.payload_columns)
        join_ts = self._key_join("T", "S")
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_EXPECTED AS

-- NO CHANGE: active in pre-SCD2 target, arrives today with same hash — should stay active
SELECT
    {key_t}
    {pay_t}
    T._RECORD_HASH,
    T.{self.strt_dt},
    '9999-12-31'::DATE AS {self.end_dt},
    FALSE              AS {self.deleted_flag},
    'NO_CHANGE'        AS EXPECTED_TYPE
FROM TMP_TGT_PREV T
INNER JOIN TMP_SRC S ON {join_ts}
WHERE T._RECORD_HASH = S._RECORD_HASH

UNION ALL

-- INSERT / UPDATE: new active row should exist with STRT_DT = BUSINESS_DATE
SELECT
    {key_c}
    {pay_c}
    C._RECORD_HASH,
    C.{self.strt_dt},
    '9999-12-31'::DATE AS {self.end_dt},
    FALSE              AS {self.deleted_flag},
    C.CHANGE_TYPE      AS EXPECTED_TYPE
FROM TMP_CLASSIFIED C
WHERE C.CHANGE_TYPE IN ('INSERT', 'UPDATE')

UNION ALL

-- DELETE: soft-delete marker should exist with DELETED_FLAG = TRUE
SELECT
    {key_c}
    {pay_c}
    C._RECORD_HASH,
    C.{self.strt_dt},
    '9999-12-31'::DATE AS {self.end_dt},
    TRUE               AS {self.deleted_flag},
    'DELETE_MARKER'    AS EXPECTED_TYPE
FROM TMP_CLASSIFIED C
WHERE C.CHANGE_TYPE = 'DELETE';
""".strip()

    def check_missing(self) -> str:
        """Finds expected rows that are absent from target_post."""
        key_e = self._col_list("E", self.keys)
        pay_e = self._col_list("E", self.payload_columns)
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_MISSING AS
SELECT
    E.EXPECTED_TYPE,
    {key_e},
    {pay_e},
    E._RECORD_HASH,
    E.{self.strt_dt},
    E.{self.end_dt},
    E.{self.deleted_flag}
FROM (
    SELECT EXPECTED_TYPE, {self._key_cols_raw}, {self._payload_cols},
           _RECORD_HASH, {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
    FROM TMP_EXPECTED

    MINUS

    SELECT NULL AS EXPECTED_TYPE, {self._key_cols_raw}, {self._payload_cols},
           {self._hash_expr()} AS _RECORD_HASH,
           {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
    FROM {self.tgt_post_fqn}
    WHERE '{self.business_date}'::DATE BETWEEN {self.strt_dt} AND {self.end_dt}
) E;
""".strip()

    def check_extra(self) -> str:
        """Finds rows written to target_post on business_date that were not expected."""
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_EXTRA AS
SELECT {self._key_cols_raw}, {self._payload_cols},
       {self._hash_expr()} AS _RECORD_HASH,
       {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
FROM (
    -- Rows actually written today in target_post (STRT_DT = BUSINESS_DATE only —
    -- not BETWEEN, which would also catch prior-run open-ended rows and cause
    -- false positives)
    SELECT {self._key_cols_raw}, {self._payload_cols},
           {self._hash_expr()} AS _RECORD_HASH,
           {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
    FROM {self.tgt_post_fqn}
    WHERE {self.strt_dt} = '{self.business_date}'::DATE

    MINUS

    SELECT {self._key_cols_raw}, {self._payload_cols},
           _RECORD_HASH, {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
    FROM TMP_EXPECTED
    WHERE {self.strt_dt} = '{self.business_date}'::DATE
) X;
""".strip()

    def check_expiry(self) -> str:
        """
        Checks that old versions in target_post were correctly expired
        (END_DT = business_date - 1) for UPDATE and DELETE change types.
        """
        key_c   = self._col_list("C", self.keys)
        join_ct = self._key_join("T", "C")
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_EXPIRY_ISSUES AS
SELECT
    {key_c},
    C.CHANGE_TYPE,
    T.{self.strt_dt}                               AS OLD_STRT_DT,
    T.{self.end_dt}                                AS ACTUAL_END_DT,
    DATEADD(DAY, -1, '{self.business_date}'::DATE) AS EXPECTED_END_DT
FROM TMP_CLASSIFIED C
INNER JOIN {self.tgt_post_fqn} T
    ON {join_ct}
    AND T.{self.deleted_flag} = FALSE
    AND T.{self.strt_dt}      < '{self.business_date}'::DATE
WHERE C.CHANGE_TYPE IN ('UPDATE', 'DELETE')
  AND T.{self.end_dt} <> DATEADD(DAY, -1, '{self.business_date}'::DATE);
""".strip()

    def summary_query(self) -> str:
        return f"""
SELECT
    '{self.tgt_post_fqn}'       AS TARGET_TABLE,
    '{self.business_date}'::DATE AS BUSINESS_DATE,
    CHECK_TYPE,
    DISCREPANCY_COUNT,
    STATUS,
    CURRENT_TIMESTAMP()         AS VALIDATED_AT
FROM (
    SELECT '1: INSERT  - new active row written'             AS CHECK_TYPE,
           COUNT(*) AS DISCREPANCY_COUNT,
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
    FROM TMP_MISSING WHERE EXPECTED_TYPE = 'INSERT'
    UNION ALL
    SELECT '2: UPDATE  - new active version written',
           COUNT(*), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM TMP_MISSING WHERE EXPECTED_TYPE = 'UPDATE'
    UNION ALL
    SELECT '3: UPDATE  - old version correctly expired',
           COUNT(*), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM TMP_EXPIRY_ISSUES WHERE CHANGE_TYPE = 'UPDATE'
    UNION ALL
    SELECT '4: DELETE  - delete marker written',
           COUNT(*), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM TMP_MISSING WHERE EXPECTED_TYPE = 'DELETE_MARKER'
    UNION ALL
    SELECT '5: DELETE  - old version correctly expired',
           COUNT(*), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM TMP_EXPIRY_ISSUES WHERE CHANGE_TYPE = 'DELETE'
    UNION ALL
    SELECT '6: NO CHANGE - unchanged rows still active',
           COUNT(*), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM TMP_MISSING WHERE EXPECTED_TYPE = 'NO_CHANGE'
    UNION ALL
    SELECT '7: NO EXTRA rows written to target',
           COUNT(*), CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM TMP_EXTRA
) SUMMARY
ORDER BY CHECK_TYPE;
""".strip()

    def create_results_table(self) -> str:
        return f"""
CREATE TABLE IF NOT EXISTS {self.res_fqn} (
    TARGET_TABLE      VARCHAR,
    BUSINESS_DATE     DATE,
    CHECK_TYPE        VARCHAR,
    DISCREPANCY_COUNT INTEGER,
    STATUS            VARCHAR,
    VALIDATED_AT      TIMESTAMP_NTZ
);
""".strip()

    def delete_existing_results(self) -> str:
        return f"""
DELETE FROM {self.res_fqn}
WHERE TARGET_TABLE  = '{self.tgt_post_fqn}'
  AND BUSINESS_DATE = '{self.business_date}'::DATE;
""".strip()

    def insert_results(self) -> str:
        return f"INSERT INTO {self.res_fqn}\n{self.summary_query()}"


# =============================================================================
# VALIDATOR — orchestrates connection, execution, results
# =============================================================================
class SCD2Validator:

    def __init__(self, cfg: dict, session=None):
        self.cfg     = validate_config(cfg)
        self.builder = SCD2SqlBuilder(self.cfg)
        self.session = session  # injected by Snowflake worksheet runtime
        self.conn    = None
        self.cur     = None

    def _connect(self):
        if self.session is not None:
            session = self.session
        else:
            from snowflake.snowpark.context import get_active_session
            session = get_active_session()
        c = self.cfg["connection"]
        log.info("Using active Snowflake session (Python Worksheet)")
        self.conn = session.connection
        self.cur  = self.conn.cursor()
        self.cur.execute(f"USE DATABASE {c['database']}")
        self.cur.execute(f"USE WAREHOUSE {c['warehouse']}")
        log.info(f"  Database  : {c['database']}")
        log.info(f"  Warehouse : {c['warehouse']}")

    def _run(self, label: str, sql: str) -> int:
        log.info(f"  {label}")
        self.cur.execute(sql)
        return self.cur.rowcount

    def _fetch(self, sql: str) -> list:
        self.cur.execute(sql)
        return self.cur.fetchall()

    def run(self) -> bool:
        b = self.builder
        try:
            self._connect()

            # Build temp tables
            self._run("STEP 1 — TMP_SRC (source snapshot)",          b.tmp_src())
            self._run("STEP 2 — TMP_TGT_PREV (pre-SCD2 snapshot)",   b.tmp_tgt_prev())
            self._run("STEP 3 — TMP_CLASSIFIED",                      b.tmp_classified())
            self._run("STEP 4 — TMP_EXPECTED",                        b.tmp_expected())

            # Run checks against target_post
            self._run("CHECK A — TMP_MISSING (vs target_post)",       b.check_missing())
            self._run("CHECK B — TMP_EXTRA   (vs target_post)",       b.check_extra())
            self._run("CHECK C — TMP_EXPIRY_ISSUES (vs target_post)", b.check_expiry())

            # Fetch and display results
            rows = self._fetch(b.summary_query())

            print()
            print("=" * 75)
            print("SCD2 VALIDATION RESULTS  [3-Table Mode]")
            print(f"  Source        : {b.src_fqn}")
            print(f"  Target Pre    : {b.tgt_pre_fqn}")
            print(f"  Target Post   : {b.tgt_post_fqn}")
            print(f"  Business Date : {b.business_date}")
            print(f"  Validated At  : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print("=" * 75)
            print(f"  {'CHECK':<45}  {'DISCREPANCIES':>13}  STATUS")
            print("-" * 75)
            all_pass = True
            for _, _, check_type, disc_count, status, _ in rows:
                flag = "  " if status == "PASS" else ">>"
                print(f"  {flag}  {check_type:<43}  {disc_count:>13}  {status}")
                if status != "PASS":
                    all_pass = False
            print("=" * 75)
            print(f"  OVERALL: {'ALL CHECKS PASSED' if all_pass else 'SOME CHECKS FAILED -- see detail tables below'}")
            print("=" * 75)

            if not all_pass:
                print()
                print("  DRILL-DOWN: run these queries in a SQL worksheet to see failing rows:")
                print("    SELECT * FROM TMP_MISSING ORDER BY EXPECTED_TYPE;")
                print("    SELECT * FROM TMP_EXTRA;")
                print("    SELECT * FROM TMP_EXPIRY_ISSUES;")

            print()

            # Write results to Snowflake
            self._run("Create results table if not exists", b.create_results_table())
            n = self._run("Delete prior results (OVERWRITE)", b.delete_existing_results())
            log.info(f"    {n} prior result row(s) removed")
            self._run("Insert new results", b.insert_results())
            log.info(f"Results written to: {b.res_fqn}")

            return all_pass

        except Exception:
            log.error("Validation failed")
            log.error(traceback.format_exc())
            raise

        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
                log.info("Connection closed")


# =============================================================================
# ENTRY POINT
# =============================================================================
def main(session: snowflake.snowpark.Session):
    validator = SCD2Validator(CONFIG, session)
    validator.run()
