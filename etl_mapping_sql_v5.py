# etl_maping_sql_v2.py
# GENERATE SQL from mapping info, store them, and validate source vs target

# "qa_db_name": "SESAME",
# "qa_schema_name": "STG_QA",
# "qa_table_name": "QA_MAP_SQL",


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
    # CHANGE: date_filter now uses a logical variable (no alias).
    # Previously: "o.order_date between ('2024-01-01' and '2024-12-31')"
    # Now:       "order_date between ('2024-01-01' and '2024-12-31')"
    "date_filter": "order_date between ('2024-01-01' and '2024-12-31')",
    # Token name to be replaced in SQL (resolved differently for source/target).
    "date_filter_token": "order_date",
    # Target-side global filter (optional)
    "target_filter_clause": "status = 'OPEN'",
    # NEW: QA / mapping table location is metadata-driven
    # If qa_db_name is None, we fall back to source_db_name (or current DB).
    "qa_db_name": None,  # e.g. "SESAME" or None to use current / source_db_name
    "qa_schema_name": "STG",  # default schema for QA table
    "qa_table_name": "QA_TEMP_MAPPING_SQL",  # table name for generated mapping SQL
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
        "target_column_name": "cust_tier",
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
        "target_column_name": "order_date",
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
        "target_column_name": "amount",
    },
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
    return (
        [p.strip() for p in s.split(",") if p and p.strip()]
        if s and not _is_null(s)
        else []
    )


def _apply_alias_token(expr, alias, table):
    actual = (alias if alias and not _is_null(alias) else table) + "."
    return re.sub(r"(?i)\balias\.", actual, expr)


def _combine_filters(*parts):
    """Return a single SQL WHERE string combined with ANDs, with parentheses around each non-empty part."""
    toks = [p.strip() for p in parts if p and str(p).strip()]
    if not toks:
        return ""
    # Parenthesize each part to avoid precedence surprises when the caller includes AND/OR
    return " AND ".join([f"({t})" for t in toks])


# NEW: helper to resolve a logical date token into an actual column expression
def _resolve_date_filter(meta, alias=None, for_target=False):
    """
    Replace the logical date token (e.g. 'order_date') with the
    actual column expression, e.g. 'o.order_date' for source,
    'order_date' for target. Handles multiple occurrences.
    If date_filter is missing/blank, returns "".
    """
    raw = (meta.get("date_filter") or "").strip()
    if not raw:
        return ""

    token = meta.get("date_filter_token", "order_date")

    # Decide replacement expression
    if for_target:
        # Target side: usually a plain column name without alias
        replacement = token
    else:
        # Source side: use alias if available, e.g. 'o.order_date'
        replacement = f"{alias}.{token}" if alias else token

    # Replace all full-word occurrences of token
    pattern = r"\b" + re.escape(token) + r"\b"
    return re.sub(pattern, replacement, raw)


_ALIAS = {
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "CHARACTER": "STRING",
    "NCHAR": "STRING",
    "NVARCHAR": "STRING",
    "TEXT": "STRING",
    "NUMBER": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "NUMERIC": "NUMERIC",
    "INT": "NUMERIC",
    "INTEGER": "NUMERIC",
    "BIGINT": "NUMERIC",
    "SMALLINT": "NUMERIC",
    "TINYINT": "NUMERIC",
    "FLOAT": "NUMERIC",
    "DOUBLE": "NUMERIC",
    "REAL": "NUMERIC",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ",
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


# NEW: helper to get QA/mapping table FQN from TABLE_META
def _qa_table_fqn(meta=None):
    """
    Build the fully qualified name for the QA/mapping table based on TABLE_META.
    Default: <qa_db_name or source_db_name>.<qa_schema_name>.<qa_table_name>
    If qa_db_name is None, db part is omitted.
    """
    if meta is None:
        meta = TABLE_META

    db = meta.get("qa_db_name")
    if _is_null(db):
        # fallback to source_db_name (might also be None, which _path can handle)
        db = meta.get("source_db_name")

    schema = meta.get("qa_schema_name")
    table = meta.get("qa_table_name", "QA_TEMP_MAPPING_SQL")

    return _path(db, schema, table)


# -------------------------- SQL Builders ------------------------------------
def build_source_sql(meta, col):
    debug = _debug_on(meta)
    db, schema = meta["source_db_name"], meta["source_schema_name"]
    join = (meta.get("source_join_clause") or "").strip()
    src_flt = (meta.get("source_filter_clause") or "").strip()
    sys = meta.get("source_system")
    pks = _split_csv(meta["source_pk_columns"])

    tbl, alias = col["source_table_name"], col.get("source_alias")
    c = col["source_column_name"]
    override, lookup = col.get("source_column_override"), col.get("source_lookup_sql")
    default, sdt, tdt = (
        col.get("source_default_value"),
        col.get("source_data_type"),
        col.get("target_data_type"),
    )
    tgt_schema, tgt_table = col["target_schema_name"], col["target_table_name"]
    out_name = col.get("target_column_name") or c

    fqn = _path(db, schema, tbl) + (f" {alias}" if alias else "")
    if debug:
        print(f"[DEBUG] fqn -> {fqn}")

    # Expression precedence
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
    else:
        expr = c

    if not _is_null(default):
        expr = f"COALESCE({expr}, {_q_lit(default)})"
        if debug:
            print(f"[DEBUG] default_value -> {default}")

    if _needs_cast(sdt, tdt):
        expr = f"CAST({expr} AS {tdt.strip()})"
        if debug:
            print(f"[DEBUG] cast_applied -> AS {tdt.strip()}")

    sel = [f"{alias}.{pk} AS {pk}" if alias else f"{pk} AS {pk}" for pk in pks]
    sel.append(f"{expr} AS {out_name}")
    select_clause = ", ".join(sel)
    if debug:
        print(f"[DEBUG] select_clause -> {select_clause}")

    # resolve variable-style date_filter for SOURCE using alias (e.g. o.order_date)
    date_flt = _resolve_date_filter(meta, alias=alias, for_target=False)

    # WHERE: combine source filter + resolved date_filter
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

    tgt_schema, tgt_table = col["target_schema_name"], col["target_table_name"]
    out_name = col.get("target_column_name") or col["source_column_name"]

    fqn = _path(db, tgt_schema, tgt_table)
    if debug:
        print(f"[DEBUG] target_fqn -> {fqn}")

    select_clause = ", ".join(
        [f"{pk} AS {pk}" for pk in pks] + [f"{out_name} AS {out_name}"]
    )
    if debug:
        print(f"[DEBUG] select_clause (target) -> {select_clause}")

    # resolve variable-style date_filter for TARGET (typically plain column name)
    date_flt = _resolve_date_filter(meta, alias=None, for_target=True)

    # WHERE: combine target filter + resolved date_filter
    where_combined = _combine_filters(tgt_flt, date_flt)
    if debug and where_combined:
        print(f"[DEBUG] where_clause (target) -> {where_combined}")

    sql = [f"SELECT {select_clause}", f"FROM {fqn}"]
    if where_combined:
        sql.append("WHERE " + where_combined)
    return "\n".join(sql) + ";"


# --------------------------- Main Entry (Generator) -------------------------
def main_generate(session: Session):
    """
    Generate mapping SQL into the QA/mapping table and return its contents.
    Location is driven by TABLE_META (qa_db_name, qa_schema_name, qa_table_name).
    """
    qa_table = _qa_table_fqn()

    session.sql(f"""
        CREATE OR REPLACE TRANSIENT TABLE {qa_table} (
            COLUMN_NAME       STRING,
            SQL_TEXT          STRING,
            TARGET_TABLE_SQL  STRING
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

    # Return DataFrame with generated mapping SQL
    return session.sql(f"SELECT * FROM {qa_table} ORDER BY COLUMN_NAME")


# -------------- EXECUTOR / VALIDATOR FUNCTIONS --------


# ============================================================
# Utility: ensure required columns exist
# ============================================================
def _ensure_validation_columns(session, table_fqn: str):
    """
    Ensures the QA table has identity and result columns.
    """
    # Stable surrogate key
    session.sql(f"""
        ALTER TABLE {table_fqn}
        ADD COLUMN IF NOT EXISTS ROW_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1
    """).collect()

    # SQLs, results, and error tracking
    session.sql(f"""
        ALTER TABLE {table_fqn}
        ADD COLUMN IF NOT EXISTS
            COUNT_SQL         STRING,
            DIFF_SQL          STRING,
            COUNT_RESULT_JSON VARIANT,
            DIFF_RESULT       NUMBER,
            COUNT_ERROR       STRING,
            DIFF_ERROR        STRING
    """).collect()


# ============================================================
# Function A: Prepare validation SQLs
# ============================================================
def prepare_validation_sqls(session, table_fqn: str = ""):
    """
    A) Generate and store COUNT_SQL & DIFF_SQL per row.
       COUNT_SQL: count of (SQL_TEXT) UNION ALL count of (TARGET_TABLE_SQL)
       DIFF_SQL : count of ( (SQL_TEXT) MINUS (TARGET_TABLE_SQL) )
    Resets prior results/errors.

    If table_fqn is empty, uses TABLE_META (qa_* settings).
    """
    if not table_fqn:
        table_fqn = _qa_table_fqn()

    _ensure_validation_columns(session, table_fqn)

    rows = session.sql(f"""
        SELECT ROW_ID, COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL
        FROM {table_fqn}
        ORDER BY ROW_ID
    """).collect()

    for r in rows:
        row_id = r["ROW_ID"]
        src_sql = r["SQL_TEXT"]
        tgt_sql = r["TARGET_TABLE_SQL"]

        # Skip if missing either side
        if not src_sql or not tgt_sql:
            continue

        src_inner = str(src_sql).rstrip(" ;\n\t")
        tgt_inner = str(tgt_sql).rstrip(" ;\n\t")

        # 1️⃣ COUNT SQL (SRC vs TGT)
        count_sql = (
            "SELECT 'SRC' AS SIDE, COUNT(*) AS CNT FROM ("
            + src_inner
            + ") SRC_T\nUNION ALL\nSELECT 'TGT' AS SIDE, COUNT(*) AS CNT FROM ("
            + tgt_inner
            + ") TGT_T"
        )

        # 2️⃣ DIFF SQL (SRC MINUS TGT)
        diff_sql = (
            "SELECT COUNT(*) AS DIFF_CNT FROM (\n("
            + src_inner
            + ")\nMINUS\n("
            + tgt_inner
            + ")\n) D"
        )

        # Escape quotes for UPDATE
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


# ============================================================
# Function B: Run validation SQLs and capture results/errors
# ============================================================
def run_validation_sqls(session, table_fqn: str = ""):
    """
    B) Execute the prepared SQLs per row and store results.
       - COUNT_RESULT_JSON: {"SRC": n1, "TGT": n2}
       - DIFF_RESULT: rows in SRC but not in TGT
       - COUNT_ERROR / DIFF_ERROR: error message if SQL fails
    Execution continues even if some rows fail.

    If table_fqn is empty, uses TABLE_META (qa_* settings).
    """
    if not table_fqn:
        table_fqn = _qa_table_fqn()

    _ensure_validation_columns(session, table_fqn)

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

        # Initialize placeholders
        counts = {"SRC": None, "TGT": None}
        count_err = None
        diff_val = None
        diff_err = None

        # 1️⃣ Execute COUNT_SQL
        try:
            cnt_rows = session.sql(count_sql).collect()
            tmp = {"SRC": 0, "TGT": 0}
            for cr in cnt_rows:
                side = str(cr["SIDE"]).upper()
                val = int(cr["CNT"])
                if side in tmp:
                    tmp[side] = val
            counts = tmp
        except Exception as e:
            count_err = str(e)[:4000].replace("'", "''")

        # 2️⃣ Execute DIFF_SQL
        try:
            drows = session.sql(diff_sql).collect()
            diff_val = int(drows[0]["DIFF_CNT"]) if drows else 0
        except Exception as e:
            diff_err = str(e)[:4000].replace("'", "''")

        # 3️⃣ Build update expressions
        counts_json_expr = (
            f"""PARSE_JSON('{{"SRC": {counts["SRC"]}, "TGT": {counts["TGT"]}}}')"""
            if (
                counts["SRC"] is not None
                and counts["TGT"] is not None
                and count_err is None
            )
            else "NULL"
        )
        diff_expr = "NULL" if diff_val is None else str(diff_val)
        cerr_expr = "NULL" if count_err is None else f"'{count_err}'"
        derr_expr = "NULL" if diff_err is None else f"'{diff_err}'"

        # 4️⃣ Update row safely
        session.sql(f"""
            UPDATE {table_fqn}
            SET COUNT_RESULT_JSON = {counts_json_expr},
                DIFF_RESULT       = {diff_expr},
                COUNT_ERROR       = {cerr_expr},
                DIFF_ERROR        = {derr_expr}
            WHERE ROW_ID = {row_id}
        """).collect()


# ============================================================
# Validation entrypoint (validate existing mappings)
# ============================================================
def main_validate(session: Session):
    """
    Validation pipeline:
      1️ Prepare validation SQLs for each mapping (COUNT_SQL, DIFF_SQL)
      2️ Execute and store results/errors
      3️ Return summary DataFrame for review
    Uses QA table FQN derived from TABLE_META.
    """
    qa_table = _qa_table_fqn()

    prepare_validation_sqls(session, qa_table)
    print("Validation SQLs prepared.")

    run_validation_sqls(session, qa_table)
    print("Validation SQLs executed.")

    # Return DataFrame summary for worksheet display
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


# ============================================================
# Orchestrator main: generate + validate
# ============================================================
def main(session: Session):
    """
    Orchestrator:
      1️ Generate mapping SQL into QA/mapping table (from TABLE_META)
      2️ Prepare and execute validation SQLs
      3️ Return validation summary DataFrame
    """
    # Step 1: generate mapping SQL
    main_generate(session)

    # Step 2: validate mappings
    return main_validate(session)


# ============================================================
# Run interactively in worksheet (choose one)
# ============================================================
# main_generate(session)   # Only generate mapping SQL into QA table
# main_validate(session)   # Only validate already-generated mappings
main(session)  # Orchestrate: generate + validate, return validation summary

# HOW TO RUN MANUALLY (if needed):
# qa_table = _qa_table_fqn()
# prepare_validation_sqls(session, qa_table)
# run_validation_sqls(session, qa_table)
#
# session.sql(f"""
#   SELECT ROW_ID, COLUMN_NAME, COUNT_RESULT_JSON, DIFF_RESULT, COUNT_ERROR, DIFF_ERROR
#   FROM {qa_table}
#   ORDER BY ROW_ID
# """).show()
