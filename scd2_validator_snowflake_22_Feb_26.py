"""
scd2_validator_snowflake.py
===========================
SCD2 post-load validation tool — single file edition for Snowflake Python Worksheet.

HOW TO USE:
  1. Fill in the CONFIG dict below (the only section you need to edit)
  2. Paste this entire file into a Snowflake Python Worksheet
  3. Click Run

NO CREDENTIALS NEEDED:
  Runs inside Snowflake using the active worksheet session.
  Just specify the database and warehouse to use.

WHAT IT DOES:
  Validates that SCD2 was correctly applied to your target table for a given
  business date. Reads source and target (never writes to either), simulates
  the expected SCD2 output in temp tables, then compares against actuals.
  Results are written to a configurable Snowflake results table.

7 CHECKS PERFORMED:
  1. INSERT  — new active row written
  2. UPDATE  — new active version written
  3. UPDATE  — old version correctly expired (END_DT = business_date - 1)
  4. DELETE  — delete marker written
  5. DELETE  — old version correctly expired (END_DT = business_date - 1)
  6. NO CHANGE — unchanged rows still active
  7. NO EXTRA rows written to target

RESULTS TABLE:
  Created automatically if it does not exist.
  Results for the same TARGET_TABLE + BUSINESS_DATE are replaced on each run.
"""

# =============================================================================
# !! CONFIGURE HERE — the only section you need to edit !!
# =============================================================================
CONFIG = {
    "connection": {
        "database": "your_database",  # database containing source and target
        "warehouse": "your_warehouse",  # warehouse to use for the run
    },
    "run": {
        "business_date": "2026-02-21",  # date being validated (YYYY-MM-DD)
    },
    "source": {
        "schema": "SRC_SCHEMA",
        "table": "SRC_TABLE",
    },
    "target": {
        "schema": "TGT_SCHEMA",
        "table": "TGT_TABLE",
        # SCD2 control column names — change only if your table uses different names
        "scd2_columns": {
            "strt_dt": "STRT_DT",
            "end_dt": "END_DT",
            "deleted_flag": "DELETED_FLAG",
        },
    },
    # Natural / composite key columns
    "keys": ["KEY_COL1", "KEY_COL2"],
    # Columns to hash for change detection
    # Exclude: key cols, audit cols, SCD2 control cols (STRT_DT, END_DT, etc.)
    "payload_columns": ["COL_A", "COL_B", "COL_C"],
    # Audit columns — listed here for documentation only (not used in hashing)
    # No RECORD_HASH or SK columns are required in source or target
    "audit_columns": ["CREATED_AT", "UPDATED_AT", "BATCH_ID"],
    "results": {
        "schema": "AUDIT_SCHEMA",
        "table": "SCD2_VALIDATION_RESULTS",
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
log = logging.getLogger("scd2_validator")


# =============================================================================
# CONFIG VALIDATOR
# =============================================================================
def validate_config(cfg: dict):
    errors = []

    for section in (
        "connection",
        "run",
        "source",
        "target",
        "keys",
        "payload_columns",
        "results",
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

    if "target" in cfg:
        for field in ("schema", "table"):
            if not cfg["target"].get(field):
                errors.append(f"target.{field} is required")

    if "keys" in cfg and not cfg["keys"]:
        errors.append("keys must contain at least one column name")

    if "payload_columns" in cfg and not cfg["payload_columns"]:
        errors.append("payload_columns must contain at least one column name")

    if "results" in cfg:
        for field in ("schema", "table"):
            if not cfg["results"].get(field):
                errors.append(f"results.{field} is required")

    if errors:
        for e in errors:
            log.error(f"Config error: {e}")
        raise ValueError(f"Config has {len(errors)} error(s) — see log above")

    # Apply SCD2 column name defaults
    scd2_defaults = {
        "strt_dt": "STRT_DT",
        "end_dt": "END_DT",
        "deleted_flag": "DELETED_FLAG",
    }
    user_scd2 = cfg["target"].get("scd2_columns", {})
    cfg["target"]["scd2_columns"] = {**scd2_defaults, **user_scd2}

    cfg.setdefault("audit_columns", [])

    log.info("Config validated successfully")
    return cfg


# =============================================================================
# SQL BUILDER
# =============================================================================
class SCD2SqlBuilder:
    """Builds all validation SQL from config. No hardcoded column names."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.db = cfg["connection"]["database"]
        self.business_date = cfg["run"]["business_date"]

        self.src_schema = cfg["source"]["schema"]
        self.src_table = cfg["source"]["table"]
        self.src_fqn = f"{self.db}.{self.src_schema}.{self.src_table}"

        self.tgt_schema = cfg["target"]["schema"]
        self.tgt_table = cfg["target"]["table"]
        self.tgt_fqn = f"{self.db}.{self.tgt_schema}.{self.tgt_table}"

        scd2 = cfg["target"]["scd2_columns"]
        self.strt_dt = scd2["strt_dt"]
        self.end_dt = scd2["end_dt"]
        self.deleted_flag = scd2["deleted_flag"]

        self.keys = cfg["keys"]
        self.payload_columns = cfg["payload_columns"]
        self.audit_columns = cfg.get("audit_columns", [])

        self.res_schema = cfg["results"]["schema"]
        self.res_table = cfg["results"]["table"]
        self.res_fqn = f"{self.db}.{self.res_schema}.{self.res_table}"

        self._key_cols_raw = ", ".join(self.keys)
        self._payload_cols = ", ".join(self.payload_columns)

    def _hash_expr(self, table_alias: str = None) -> str:
        """SHA2 hash expression over payload columns, optionally qualified by alias."""
        prefix = f"{table_alias}." if table_alias else ""
        parts = [
            f"COALESCE(CAST({prefix}{col} AS VARCHAR), '')"
            for col in self.payload_columns
        ]
        inner = ",\n        ".join(parts)
        return f"SHA2(CONCAT_WS('||',\n        {inner}\n    ), 256)"

    def _key_join(self, left: str, right: str) -> str:
        """ON clause joining two aliases on all key columns."""
        return "\n    AND ".join(f"{left}.{k} = {right}.{k}" for k in self.keys)

    def _col_list(self, alias: str, cols: list) -> str:
        return ",\n    ".join(f"{alias}.{c}" for c in cols)

    # -------------------------------------------------------------------------
    def tmp_src(self) -> str:
        key_cols = "\n    ".join(f"{c}," for c in self.keys)
        payload = "\n    ".join(f"{c}," for c in self.payload_columns)
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_SRC AS
SELECT
    {key_cols}
    {payload}
    {self._hash_expr()} AS _RECORD_HASH,
    '{self.business_date}'::DATE AS {self.strt_dt}
FROM {self.src_fqn}
WHERE '{self.business_date}'::DATE BETWEEN {self.strt_dt} AND {self.end_dt}
  AND {self.deleted_flag} = FALSE;
""".strip()

    def tmp_tgt_prev(self) -> str:
        key_cols = "\n    ".join(f"T.{c}," for c in self.keys)
        payload = "\n    ".join(f"T.{c}," for c in self.payload_columns)
        key_null_chk = " AND ".join(f"T2.{k} = T.{k}" for k in self.keys)
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_TGT_PREV AS
SELECT
    {key_cols}
    {payload}
    {self._hash_expr('T')} AS _RECORD_HASH,
    T.{self.strt_dt},
    T.{self.end_dt}
FROM {self.tgt_fqn} T
WHERE DATEADD(DAY, -1, '{self.business_date}'::DATE)
      BETWEEN T.{self.strt_dt} AND T.{self.end_dt}
  AND T.{self.deleted_flag} = FALSE
  AND NOT EXISTS (
      -- Exclude keys already carrying an active delete marker from a prior run.
      -- Without this, a record deleted on D-1 (expired row END_DT = D-1) would
      -- slip into TMP_TGT_PREV and be misclassified as UPDATE instead of INSERT
      -- when it re-arrives in source today.
      SELECT 1
      FROM {self.tgt_fqn} T2
      WHERE {key_null_chk}
        AND T2.{self.deleted_flag} = TRUE
        AND T2.{self.end_dt}       = '9999-12-31'
  );
""".strip()

    def tmp_classified(self) -> str:
        key_s = "\n    ".join(f"S.{c}," for c in self.keys)
        key_t = "\n    ".join(f"T.{c}," for c in self.keys)
        pay_s = "\n    ".join(f"S.{c}," for c in self.payload_columns)
        pay_t = "\n    ".join(f"T.{c}," for c in self.payload_columns)
        join_st = self._key_join("S", "T")
        null_k = self.keys[0]
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_CLASSIFIED AS

-- INSERT: in source today, absent from D-1 target
SELECT
    {key_s}
    {pay_s}
    S._RECORD_HASH,
    S.{self.strt_dt},
    'INSERT' AS CHANGE_TYPE
FROM TMP_SRC S
LEFT JOIN TMP_TGT_PREV T ON {join_st}
WHERE T.{null_k} IS NULL

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

-- DELETE: in D-1 target, absent from source today
SELECT
    {key_t}
    {pay_t}
    T._RECORD_HASH,
    '{self.business_date}'::DATE AS {self.strt_dt},
    'DELETE' AS CHANGE_TYPE
FROM TMP_TGT_PREV T
LEFT JOIN TMP_SRC S ON {self._key_join('T', 'S')}
WHERE S.{null_k} IS NULL;
""".strip()

    def tmp_expected(self) -> str:
        key_t = "\n    ".join(f"T.{c}," for c in self.keys)
        pay_t = "\n    ".join(f"T.{c}," for c in self.payload_columns)
        key_c = "\n    ".join(f"C.{c}," for c in self.keys)
        pay_c = "\n    ".join(f"C.{c}," for c in self.payload_columns)
        join_ts = self._key_join("T", "S")
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_EXPECTED AS

-- NO CHANGE: active in D-1, arrives today with same hash — should stay active
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
    FROM {self.tgt_fqn}
    WHERE '{self.business_date}'::DATE BETWEEN {self.strt_dt} AND {self.end_dt}
) E;
""".strip()

    def check_extra(self) -> str:
        return f"""
CREATE OR REPLACE TEMPORARY TABLE TMP_EXTRA AS
SELECT {self._key_cols_raw}, {self._payload_cols},
       {self._hash_expr()} AS _RECORD_HASH,
       {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
FROM (
    -- Rows actually written today (STRT_DT = BUSINESS_DATE only — not BETWEEN,
    -- which would also catch prior-run open-ended rows and cause false positives)
    SELECT {self._key_cols_raw}, {self._payload_cols},
           {self._hash_expr()} AS _RECORD_HASH,
           {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
    FROM {self.tgt_fqn}
    WHERE {self.strt_dt} = '{self.business_date}'::DATE

    MINUS

    SELECT {self._key_cols_raw}, {self._payload_cols},
           _RECORD_HASH, {self.strt_dt}, {self.end_dt}, {self.deleted_flag}
    FROM TMP_EXPECTED
    WHERE {self.strt_dt} = '{self.business_date}'::DATE
) X;
""".strip()

    def check_expiry(self) -> str:
        key_c = self._col_list("C", self.keys)
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
INNER JOIN {self.tgt_fqn} T
    ON {join_ct}
    AND T.{self.deleted_flag} = FALSE
    AND T.{self.strt_dt}      < '{self.business_date}'::DATE
WHERE C.CHANGE_TYPE IN ('UPDATE', 'DELETE')
  AND T.{self.end_dt} <> DATEADD(DAY, -1, '{self.business_date}'::DATE);
""".strip()

    def summary_query(self) -> str:
        return f"""
SELECT
    '{self.tgt_fqn}'      AS TARGET_TABLE,
    '{self.business_date}'::DATE AS BUSINESS_DATE,
    CHECK_TYPE,
    DISCREPANCY_COUNT,
    STATUS,
    CURRENT_TIMESTAMP()   AS VALIDATED_AT
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
WHERE TARGET_TABLE  = '{self.tgt_fqn}'
  AND BUSINESS_DATE = '{self.business_date}'::DATE;
""".strip()

    def insert_results(self) -> str:
        return f"""
INSERT INTO {self.res_fqn}
{self.summary_query()}
""".strip()


# =============================================================================
# VALIDATOR — orchestrates connection, execution, results
# =============================================================================
class SCD2Validator:

    def __init__(self, cfg: dict, session=None):
        self.cfg = validate_config(cfg)
        self.builder = SCD2SqlBuilder(self.cfg)
        self.session = session  # injected by Snowflake worksheet runtime
        self.conn = None
        self.cur = None

    def _connect(self):
        # Use the session passed in by Snowflake worksheet runtime (main(session))
        # Fall back to get_active_session() if called outside a worksheet
        if self.session is not None:
            session = self.session
        else:
            from snowflake.snowpark.context import get_active_session

            session = get_active_session()
        c = self.cfg["connection"]
        log.info("Using active Snowflake session (Python Worksheet)")
        self.conn = session.connection
        self.cur = self.conn.cursor()
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
            self._run("STEP 1 — TMP_SRC", b.tmp_src())
            self._run("STEP 2 — TMP_TGT_PREV", b.tmp_tgt_prev())
            self._run("STEP 3 — TMP_CLASSIFIED", b.tmp_classified())
            self._run("STEP 4 — TMP_EXPECTED", b.tmp_expected())

            # Run checks
            self._run("CHECK A — TMP_MISSING", b.check_missing())
            self._run("CHECK B — TMP_EXTRA", b.check_extra())
            self._run("CHECK C — TMP_EXPIRY_ISSUES", b.check_expiry())

            # Fetch and display results
            rows = self._fetch(b.summary_query())

            print()
            print("=" * 75)
            print("SCD2 VALIDATION RESULTS")
            print(f"  Target        : {b.tgt_fqn}")
            print(f"  Business Date : {b.business_date}")
            print(
                f"  Validated At  : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
            print("=" * 75)
            print(f"  {'CHECK':<45}  {'DISCREPANCIES':>13}  STATUS")
            print("-" * 75)
            all_pass = True
            for _, _, check_type, disc_count, status, _ in rows:
                marker = "PASS" if status == "PASS" else "FAIL"
                flag = "  " if status == "PASS" else ">>"
                print(f"  {flag}  {check_type:<43}  {disc_count:>13}  {marker}")
                if status != "PASS":
                    all_pass = False
            print("=" * 75)
            print(
                f"  OVERALL: {'ALL CHECKS PASSED' if all_pass else 'SOME CHECKS FAILED -- see detail tables below'}"
            )
            print("=" * 75)

            if not all_pass:
                print()
                print(
                    "  DRILL-DOWN: run these queries in a SQL worksheet to see failing rows:"
                )
                print(f"    SELECT * FROM TMP_MISSING ORDER BY EXPECTED_TYPE;")
                print(f"    SELECT * FROM TMP_EXTRA;")
                print(f"    SELECT * FROM TMP_EXPIRY_ISSUES;")

            print()

            # Write results to Snowflake
            self._run("Create results table if not exists", b.create_results_table())
            n = self._run(
                "Delete prior results (OVERWRITE)", b.delete_existing_results()
            )
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
