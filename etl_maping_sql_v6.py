# etl_maping_sql_v2.py
# GENERATE SQL from mapping info, store them, and validate source vs target

from snowflake.snowpark import Session
import re

# ===================== Configuration =========================================
TABLE_META = {
    "debug_mode": "YES",  # "YES" prints debug info

    "source_system": "ERP",
    "source_db_name": "SALES_DB",
    "source_schema_name": "PUBLIC",
    "source_pk_columns": "ORDER_ID, LINE_NO",
    "source_join_clause": "LEFT JOIN LKP.CUST c ON c.id = o.cust_id",
    "source_filter_clause": "o.status = 'OPEN'",

    # date_filter uses a logical token "order_date" which will NOT be resolved
    # in generator or prepare_validation_sqls. It stays as a placeholder string
    # for later replacement in run_validation_sqls().
    # XX.START_DT and XX.END_DT are left as-is (to be resolved at execution time).
    "date_filter": "order_date between XX.START_DT and XX.END_DT",
    "date_filter_token": "order_date",

    # Target-side global filter (optional)
    "target_filter_clause": "status = 'OPEN'",

    # QA / mapping table location is metadata-driven
    # If qa_db_name is None, we fall back to source_db_name (or current DB).
    "qa_db_name": None,                     # e.g. "SESAME" or None to use source_db_name/current DB
    "qa_schema_name": "STG",                # default schema for QA table
    "qa_table_name": "QA_TEMP_MAPPING_SQL"  # table name for generated mapping SQL
}

COLUMNS = [
    {
        "source_table_name": "ORDERS",
        "source_alias": "o",
        "source_column_name": "cust_tier",
        "source_column_override": None,
        "source_lookup_sql": "(select x.tier from YY x where x.cust_id = alias.cust_id)",
        "source_default_value": "BASIC",
        "source_data_type": "VARCHAR",
        "target_data_type": "STRING",
        "target_schema_name": "CORE",
        "target_table_name": "ORDERS",
        "target_column_name": "cust_tier"
    },
    {
        "source_table_name": "ORDERS",
        "source_alias": "o",
        "source_column_name": "order_date",
        "source_column_override": None,
        "source_lookup_sql": None,
        "source_default_value": "1900-01-01",
        "source_data_type": "VARCHAR",
        "target_data_type": "DATE",
        "target_schema_name": "CORE",
        "target_table_name": "ORDERS",
        "target_column_name": "order_date"
    },
    {
        "source_table_name": "ORDERS",
        "source_alias": "o",
        "source_column_name": "amount",
        "source_column_override": None,
        "source_lookup_sql": None,
        "source_default_value": 0,
        "source_data_type": "NUMBER(18,2)",
        "target_data_type": "NUMBER(18,2)",
        "target_schema_name": "CORE",
        "target_table_name": "ORDERS",
        "target_column_name": "amount"
    }
]
# ============================================================================

# ----------------------------- Helpers --------------------------------------
def _is_null(x):
    return x is None or (isinstance(x, str) and x.strip().lower() in ("null", ""))


def _path(db, schema, table):
    parts = []
    if not _is_null(db):
        parts.append(db.strip())
    if not _is_null(schema):
        parts.append(schema.strip())
    parts.append(table.strip())
    return ".".join(parts)


def _q_lit(v):
    if _is_null(v):
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _split_csv(s):
    return [p.strip() for p in s.split(",") if p and p.strip()] if s and not _is_null(s) else []


def _apply_alias_token(expr, alias, table):
    actual = (alias if alias and not _is_null(alias) else table) + "."
    return re.sub(r'(?i)\balias\.', actual, expr)


def _combine_filters(*parts):
    """Return a single SQL WHERE string combined with ANDs, with parentheses around each non-empty part."""
    toks = [p.strip() for p in parts if p and str(p).strip()]
    if not toks:
        return ""
    return " AND ".join([f"({t})" for t in toks])


def _resolve_date_filter(meta):
    """
    Return the raw date_filter string from TABLE_META (no token substitution).
    If date_filter is missing/blank, returns "".
    """
    return (meta.get("date_filter") or "").strip()


_ALIAS = {
    "STRING": "STRING", "VARCHAR": "STRING", "CHAR": "STRING", "CHARACTER": "STRING",
    "NCHAR": "STRING", "NVARCHAR": "STRING", "TEXT": "STRING",
    "NUMBER": "NUMERIC", "DECIMAL": "NUMERIC", "NUMERIC": "NUMERIC",
    "INT": "NUMERIC", "INTEGER": "NUMERIC", "BIGINT": "NUMERIC", "SMALLINT": "NUMERIC",
    "TINYINT": "NUMERIC", "FLOAT": "NUMERIC", "DOUBLE": "NUMERIC", "REAL": "NUMERIC",
    "BOOLEAN": "BOOL", "BOOL": "BOOL", "DATE": "DATE", "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP_NTZ", "TIMESTAMP_LTZ": "TIMESTAMP_LTZ", "TIMESTAMP_TZ": "TIMESTAMP_TZ",
}
_TYPE_RE = re.compile(r"^\s*([A-Z_]+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")


def _canonical_type(t):
    if _is_null(t):
        return None
    raw = str(t).strip().upper()
    m = _TYPE_RE.match(raw)
    if not m:
        return None
    base, p, s = m.group(1), m.group(2), m.group(3)
    fam = _ALIAS.get(base, base)
    if p and s:
        return f"{fam}({int(p)},{int(s)})"
    if p:
        return f"{fam}({int(p)})"
    return fam


def _needs_cast(src_type, tgt_type):
    tgt = _canonical_type(tgt_type)
    if tgt is None:
        return False
    src = _canonical_type(src_type)
    return src != tgt


def _debug_on(meta):
    return str(meta.get("debug_mode", "")).strip().upper() == "YES"


def _qa_table_fqn(meta=None):
    """
    Build the fully qualified name for the QA/mapping table based on TABLE_META.
    Default: <qa_db_name or source_db_name>.<qa_schema_name>.<qa_table_name>
    If qa_db_name is None, db part comes from source_db_name (or may be omitted).
    """
    if meta is None:
        meta = TABLE_META

    db = meta.get("qa_db_name")
    if _is_null(db):
        db = meta.get("source_db_name")

    schema = meta.get("qa_schema_name")
    table = meta.get("qa_table_name", "QA_TEMP_MAPPING_SQL")

    return _path(db, schema, table)


def _inject_order_date(sql, order_date_value):
    """
    Replace the token 'order_date' in the SQL text with the supplied
    string literal value, quoted and escaped.

    Example:
      sql:  "... where order_date between XX.START_DT and XX.END_DT"
      val:  "2024-06-30"
      ->    "... where '2024-06-30' between XX.START_DT and XX.END_DT"
    """
    if not sql or order_date_value is None:
        return sql

    lit = "'" + str(order_date_value).replace("'", "''") + "'"
    pattern = r"\border_date\b"
    return re.sub(pattern, lit, sql)


# -------------------------- SQL Builders ------------------------------------
def build_source_sql(meta, col):
    debug = _debug_on(meta)
    db, schema = meta["source_db_name"], meta["source_schema_name"]
    join = (meta.get("source_join_clause") or "").strip()
    src_flt = (meta.get("source_filter_clause") or "").strip()
    sys = meta.get("source_system")
    pks = _split_csv(meta["source_pk_columns"])

    tbl = col["source_table_name"]
    alias = col.get("source_alias")        # used only inside expressions, not in SELECT output
    c = col["source_column_name"]
    override = col.get("source_column_override")
    lookup = col.get("source_lookup_sql")
    default = col.get("source_default_value")
    sdt = col.get("source_data_type")
    tdt = col.get("target_data_type")
    tgt_schema = col["target_schema_name"]
    tgt_table = col["target_table_name"]
    out_name = col.get("target_column_name") or c   # output column name

    fqn = _path(db, schema, tbl) + (f" {alias}" if alias else "")
    if debug:
        print(f"[DEBUG] fqn -> {fqn}")

    # Expression precedence (internal alias usage)
    if not _is_null(override):
        expr = _apply_alias_token(override.strip(), alias, tbl)
        if debug:
            print(f"[DEBUG] override_expr -> {expr}")
    elif not _is_null(lookup):
        expr = _apply_alias_token(lookup.strip(), alias, tbl)
        if debug:
            print(f"[DEBUG] lookup_sql -> {expr}")
    elif alias:
        expr = f"{alias}.{c}"
        if debug:
            print(f"[DEBUG] base_expr -> {expr}")
    else:
        expr = c
        if debug:
            print(f"[DEBUG] base_expr -> {expr}")

    if not _is_null(default):
        expr = f"COALESCE({expr}, {_q_lit(default)})"
        if debug:
            print(f"[DEBUG] default_value -> {default}")

    if _needs_cast(sdt, tdt):
        expr = f"CAST({expr} AS {tdt.strip()})"
        if debug:
            print(f"[DEBUG] cast_applied -> AS {tdt.strip()}")

    # SELECT clause: no alias exposure for PKs, data column always as target_column_name
    sel = [f"{pk} AS {pk}" for pk in pks]
    sel.append(f"{expr} AS {out_name}")

    select_clause = ", ".join(sel)
    if debug:
        print(f"[DEBUG] select_clause -> {select_clause}")

    # Use raw date_filter as-is (contains 'order_date' token and XX.* tokens)
    date_flt = _resolve_date_filter(meta)

    # WHERE: combine source filter + date_filter
    where_combined = _combine_filters(src_flt, date_flt)
    if debug and where_combined:
        print(f"[DEBUG] where_clause -> {where_combined}")

    sql = []
    if sys:
        sql.append(f"-- Source System: {sys}")
    sql.append(f"-- Target: {tgt_schema}.{tgt_table}")
    sql.append(f"SELECT {select_clause}")
    sql.append(f"FROM {fqn}")
    if join:
        sql.append(join)
    if where_combined:
        sql.append("WHERE " + where_combined)
    return "\n".join(sql) + ";"


def build_target_sql(meta, col):
    debug = _debug_on(meta)
    db = meta["source_db_name"]
    tgt_flt = (meta.get("target_filter_clause") or "").strip()
    pks = _split_csv(meta["source_pk_columns"])

    tgt_schema = col["target_schema_name"]
    tgt_table = col["target_table_name"]
    out_name = col.get("target_column_name") or col["source_column_name"]

    fqn = _path(db, tgt_schema, tgt_table)
    if debug:
        print(f"[DEBUG] target_fqn -> {fqn}")

    select_clause = ", ".join([f"{pk} AS {pk}" for pk in pks] + [f"{out_name} AS {out_name}"])
    if debug:
        print(f"[DEBUG] select_clause (target) -> {select_clause}")

    # Use raw date_filter as-is (contains 'order_date' token and XX.* tokens)
    date_flt = _resolve_date_filter(meta)

    # WHERE: combine target filter + date_filter
    where_combined = _combine_filters(tgt_flt, date_flt)
    if debug and where_combined:
        print(f"[DEBUG] where_clause (target) -> {where_combined}")

    sql = [f"SELECT {select_clause}", f"FROM {fqn}"]
    if where_combined:
        sql.append("WHERE " + where_combined)
    return "\n".join(sql) + ";"


# --------------------------- Generator --------------------------------------
def main_generate(session):
    """
    Generate mapping SQL into the QA/mapping table and return its contents.
    Location is driven by TABLE_META (qa_db_name, qa_schema_name, qa_table_name).
    """
    qa_table = _qa_table_fqn()

    # All required columns are created here; no ALTER TABLE is used later.
    session.sql(f"""
        CREATE OR REPLACE TRANSIENT TABLE {qa_table} (
            ROW_ID            NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            COLUMN_NAME       STRING,
            SQL_TEXT          STRING,
            TARGET_TABLE_SQL  STRING,
            COUNT_SQL         STRING,
            DIFF_SQL          STRING,
            COUNT_RESULT_JSON VARIANT,
            DIFF_RESULT       NUMBER,
            COUNT_ERROR       STRING,
            DIFF_ERROR        STRING
        )
    """).collect()

    for col in COLUMNS:
        column_label = col.get("target_column_name") or col.get("source_column_name")
        src_sql = build_source_sql(TABLE_META, col)
        tgt_sql = build_target_sql(TABLE_META, col)

        col_esc = column_label.replace("'", "''")
        src_esc = src_sql.replace("'", "''")
        tgt_esc = tgt_sql.replace("'", "''")
        session.sql(f"""
            INSERT INTO {qa_table} (COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL)
            VALUES ('{col_esc}', '{src_esc}', '{tgt_esc}')
        """).collect()

    return session.sql(f"SELECT * FROM {qa_table} ORDER BY COLUMN_NAME")


# --------------------------- Validator --------------------------------------
def prepare_validation_sqls(session, table_fqn=""):
    """
    Generate and store COUNT_SQL and DIFF_SQL per row.
      - COUNT_SQL: count of (SQL_TEXT) UNION ALL count of (TARGET_TABLE_SQL)
      - DIFF_SQL : count of (SQL_TEXT MINUS TARGET_TABLE_SQL)
    Resets prior results/errors.

    If table_fqn is empty, uses TABLE_META (qa_* settings).
    """
    if not table_fqn:
        table_fqn = _qa_table_fqn()

    rows = session.sql(f"""
        SELECT ROW_ID, COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL
        FROM {table_fqn}
        ORDER BY ROW_ID
    """).collect()

    for r in rows:
        row_id = r["ROW_ID"]
        src_sql = r["SQL_TEXT"]
        tgt_sql = r["TARGET_TABLE_SQL"]

        if not src_sql or not tgt_sql:
            continue

        src_inner = str(src_sql).rstrip(" ;\n\t")
        tgt_inner = str(tgt_sql).rstrip(" ;\n\t")

        count_sql = (
            "SELECT 'SRC' AS SIDE, COUNT(*) AS CNT FROM ("
            + src_inner
            + ") SRC_T\nUNION ALL\nSELECT 'TGT' AS SIDE, COUNT(*) AS CNT FROM ("
            + tgt_inner
            + ") TGT_T"
        )

        diff_sql = (
            "SELECT COUNT(*) AS DIFF_CNT FROM (\n("
            + src_inner
            + ")\nMINUS\n("
            + tgt_inner
            + ")\n) D"
        )

        count_sql_esc = count_sql.replace("'", "''")
        diff_sql_esc = diff_sql.replace("'", "''")

        session.sql(f"""
            UPDATE {table_fqn}
            SET COUNT_SQL = '{count_sql_esc}',
                DIFF_SQL  = '{diff_sql_esc}',
                COUNT_RESULT_JSON = NULL,
                DIFF_RESULT       = NULL,
                COUNT_ERROR       = NULL,
                DIFF_ERROR        = NULL
            WHERE ROW_ID = {row_id}
        """).collect()


def run_validation_sqls(session, table_fqn="", order_date_value=None):
    """
    Execute the prepared SQLs per row and store results.
      - COUNT_RESULT_JSON: {"SRC": n1, "TGT": n2}
      - DIFF_RESULT: rows in SRC but not in TGT
      - COUNT_ERROR / DIFF_ERROR: error message if SQL fails

    Execution continues even if some rows fail.
    If table_fqn is empty, uses TABLE_META (qa_* settings).

    order_date_value: optional string literal to substitute for the token
                      'order_date' in COUNT_SQL and DIFF_SQL at execution time.
                      If None, the query will still contain the token 'order_date'.
    """
    if not table_fqn:
        table_fqn = _qa_table_fqn()

    rows = session.sql(f"""
        SELECT ROW_ID, COLUMN_NAME, COUNT_SQL, DIFF_SQL
        FROM {table_fqn}
        ORDER BY ROW_ID
    """).collect()

    for r in rows:
        row_id = r["ROW_ID"]
        count_sql = r["COUNT_SQL"]
        diff_sql = r["DIFF_SQL"]

        if not count_sql or not diff_sql:
            continue

        # Apply order_date substitution only for execution (templates remain unchanged in table)
        exec_count_sql = _inject_order_date(count_sql, order_date_value)
        exec_diff_sql = _inject_order_date(diff_sql, order_date_value)

        counts = {"SRC": None, "TGT": None}
        count_err = None
        diff_val = None
        diff_err = None

        # Execute COUNT_SQL
        try:
            cnt_rows = session.sql(exec_count_sql).collect()
            tmp = {"SRC": 0, "TGT": 0}
            for cr in cnt_rows:
                side = str(cr["SIDE"]).upper()
                val = int(cr["CNT"])
                if side in tmp:
                    tmp[side] = val
            counts = tmp
        except Exception as e:
            count_err = str(e)[:4000].replace("'", "''")

        # Execute DIFF_SQL
        try:
            drows = session.sql(exec_diff_sql).collect()
            diff_val = int(drows[0]["DIFF_CNT"]) if drows else 0
        except Exception as e:
            diff_err = str(e)[:4000].replace("'", "''")

        counts_json_expr = (
            f"""PARSE_JSON('{{"SRC": {counts["SRC"]}, "TGT": {counts["TGT"]}}}')"""
            if (counts["SRC"] is not None and counts["TGT"] is not None and count_err is None)
            else "NULL"
        )
        diff_expr = "NULL" if diff_val is None else str(diff_val)
        cerr_expr = "NULL" if count_err is None else f"'{count_err}'"
        derr_expr = "NULL" if diff_err is None else f"'{diff_err}'"

        session.sql(f"""
            UPDATE {table_fqn}
            SET COUNT_RESULT_JSON = {counts_json_expr},
                DIFF_RESULT       = {diff_expr},
                COUNT_ERROR       = {cerr_expr},
                DIFF_ERROR        = {derr_expr}
            WHERE ROW_ID = {row_id}
        """).collect()


# --------------------------- Public entrypoints -----------------------------
def main_validate(session, order_date_value=None):
    """
    Validation pipeline:
      - Prepare validation SQLs for each mapping (COUNT_SQL, DIFF_SQL)
      - Execute and store results/errors
      - Return summary DataFrame for review

    Uses QA table FQN derived from TABLE_META.
    order_date_value: optional string to substitute for 'order_date'
                      in COUNT_SQL and DIFF_SQL at execution time.
    """
    qa_table = _qa_table_fqn()

    prepare_validation_sqls(session, qa_table)
    print("Validation SQLs prepared.")

    run_validation_sqls(session, qa_table, order_date_value=order_date_value)
    print("Validation SQLs executed.")

    return session.sql(f"""
        SELECT
            ROW_ID,
            COLUMN_NAME,
            COUNT_RESULT_JSON,
            DIFF_RESULT,
            COUNT_ERROR,
            DIFF_ERROR
        FROM {qa_table}
        ORDER BY ROW_ID
    """)


def main(session, order_date_value=None):
    """
    Orchestrator:
      - Generate mapping SQL into QA/mapping table (from TABLE_META)
      - Prepare and execute validation SQLs
      - Return validation summary DataFrame

    order_date_value: optional string to substitute for 'order_date'
                      when executing the validation SQLs.
    """
    main_generate(session)
    return main_validate(session, order_date_value=order_date_value)


# ============================================================
# Usage examples (for worksheet)
# ============================================================
# result_df = main_generate(session)                  # Only generate mapping SQL into QA table
# result_df = main_validate(session, '2024-06-30')    # Only validate for a given order_date value
# result_df = main(session, '2024-06-30')             # Orchestrate: generate + validate for that order_date
# result_df.show()
