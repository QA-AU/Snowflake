etl_mapping_sql.py

# ===================== Configure your mapping here ============================
table_meta = {
    "debug_mode": "YES",                     # "YES" for debug prints
    "source_system": "ERP",
    "source_db_name": "SALES_DB",
    "source_schema_name": "PUBLIC",
    "source_pk_columns": "ORDER_ID, LINE_NO",
    "source_join_clause": "LEFT JOIN LKP.CUST c ON c.id = o.cust_id",  # optional
    "source_filter_clause": "o.status = 'OPEN'",                       # optional
    "target_filter_clause": "status = 'OPEN'"                          # optional (global target WHERE)
}

columns = [
    {
        "source_table_name": "ORDERS",
        "source_alias": "o",
        "source_column_name": "cust_tier",
        # NEW: when present, this wins over lookup/alias.column
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
        # Example override usage:
        "source_column_override": None,  # e.g. "TO_DATE(alias.order_date_text, 'YYYY-MM-DD')"
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
        "source_default_value": 0,
        "source_data_type": "NUMBER(18,2)",
        "target_data_type": "NUMBER(18,2)",
        "target_schema_name": "CORE",
        "target_table_name": "ORDERS",
        "target_column_name": "amount"
    }
]
# ============================================================================


# ----------------------------- Helpers (stdlib only) --------------------------
import re

def _is_null(x):
    return x is None or (isinstance(x, str) and x.strip().lower() in ("null", ""))

def _path(db, schema, table):
    parts = []
    if not _is_null(db): parts.append(db.strip())
    if not _is_null(schema): parts.append(schema.strip())
    parts.append(table.strip())
    return ".".join(parts)

def _q_lit(v):
    if _is_null(v): return "NULL"
    if isinstance(v, (int, float)): return str(v)
    return "'" + str(v).replace("'", "''") + "'"

def _split_csv(s):
    return [p.strip() for p in s.split(",") if p and p.strip()] if s and not _is_null(s) else []

def _apply_alias_token(expr, source_alias, source_table_name):
    # Replace 'alias.' (case-insensitive) with actual alias/table + '.'
    actual = (source_alias if source_alias and not _is_null(source_alias) else source_table_name) + "."
    return re.sub(r'(?i)\balias\.', actual, expr)

# Canonical type matching (exact)
_ALIAS = {
    "STRING":"STRING","VARCHAR":"STRING","CHAR":"STRING","CHARACTER":"STRING","NCHAR":"STRING","NVARCHAR":"STRING","TEXT":"STRING",
    "NUMBER":"NUMERIC","DECIMAL":"NUMERIC","NUMERIC":"NUMERIC","INT":"NUMERIC","INTEGER":"NUMERIC","BIGINT":"NUMERIC","SMALLINT":"NUMERIC","TINYINT":"NUMERIC",
    "FLOAT":"NUMERIC","DOUBLE":"NUMERIC","REAL":"NUMERIC",
    "BOOLEAN":"BOOL","BOOL":"BOOL",
    "DATE":"DATE","TIME":"TIME",
    "TIMESTAMP":"TIMESTAMP_NTZ","TIMESTAMP_NTZ":"TIMESTAMP_NTZ","TIMESTAMP_LTZ":"TIMESTAMP_LTZ","TIMESTAMP_TZ":"TIMESTAMP_TZ",
    "VARIANT":"VARIANT","OBJECT":"OBJECT","ARRAY":"ARRAY","BINARY":"BINARY","GEOGRAPHY":"GEOGRAPHY",
}
_TYPE_RE = re.compile(r"^\s*([A-Z_]+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")

def _canonical_type(t):
    if _is_null(t): return None
    raw = str(t).strip().upper()
    raw = raw.split()[0]
    m = _TYPE_RE.match(raw)
    if not m: return None
    base, p, s = m.group(1), m.group(2), m.group(3)
    fam = _ALIAS.get(base, base)
    if p and s: return f"{fam}({int(p)},{int(s)})"
    if p: return f"{fam}({int(p)})"
    return fam

def _needs_cast(src_type, tgt_type):
    tgt = _canonical_type(tgt_type)
    if tgt is None: return False
    src = _canonical_type(src_type)
    return src != tgt  # exact match required

def _debug_on(meta):
    return str(meta.get("debug_mode", "")).strip().upper() == "YES"


# -------------------------- Build SOURCE-side SQL -----------------------------
def build_source_sql(table_meta, col):
    debug = _debug_on(table_meta)

    # Shared source context
    source_db_name       = table_meta.get("source_db_name")
    source_schema_name   = table_meta.get("source_schema_name")
    source_join_clause   = (table_meta.get("source_join_clause") or "").strip()
    source_filter_clause = (table_meta.get("source_filter_clause") or "").strip()
    source_system        = table_meta.get("source_system")
    source_pk_columns    = _split_csv(table_meta.get("source_pk_columns"))

    # Per-column source context
    source_table_name      = col.get("source_table_name")
    source_alias           = col.get("source_alias")
    source_column_name     = col.get("source_column_name")
    source_column_override = col.get("source_column_override")
    source_lookup_sql      = col.get("source_lookup_sql")
    source_default_value   = col.get("source_default_value")
    source_data_type       = col.get("source_data_type")
    target_data_type       = col.get("target_data_type")

    # Per-column target context (for header)
    target_schema_name   = col.get("target_schema_name")
    target_table_name    = col.get("target_table_name")
    out_name             = col.get("target_column_name") or source_column_name

    if _is_null(source_table_name):
        raise ValueError("source_table_name is required in each column")

    fqn = _path(source_db_name, source_schema_name, source_table_name) + (f" {source_alias}" if source_alias and not _is_null(source_alias) else "")
    if debug: print(f"[DEBUG] fqn -> {fqn}")

    # ----- Expression precedence -----
    if not _is_null(source_column_override):
        expr = _apply_alias_token(str(source_column_override).strip(), source_alias, source_table_name)
        if debug: print(f"[DEBUG] source_column_override -> {expr}")
    elif not _is_null(source_lookup_sql):
        expr = _apply_alias_token(source_lookup_sql.strip(), source_alias, source_table_name)
        if debug: print(f"[DEBUG] source_lookup_sql -> {expr}")
    elif source_alias and not _is_null(source_alias):
        expr = f"{source_alias}.{source_column_name}"
        if debug: print(f"[DEBUG] base_expr -> {expr}")
    else:
        expr = source_column_name
        if debug: print(f"[DEBUG] base_expr -> {expr}")

    # Apply default if provided
    if not _is_null(source_default_value):
        expr = f"COALESCE({expr}, {_q_lit(source_default_value)})"
        if debug: print(f"[DEBUG] default_value -> {source_default_value}")

    # Exact-match cast if required
    if _needs_cast(source_data_type, target_data_type):
        expr = f"CAST({expr} AS {target_data_type.strip()})"
        if debug: print(f"[DEBUG] cast_applied -> AS {target_data_type.strip()}")

    # SELECT list: PKs first, then the column
    select_parts = []
    for pk in source_pk_columns:
        pk_expr = f"{source_alias}.{pk}" if source_alias and not _is_null(source_alias) else pk
        select_parts.append(f"{pk_expr} AS {pk}")
    select_parts.append(f"{expr} AS {out_name}")
    select_clause = ", ".join(select_parts)
    if debug: print(f"[DEBUG] select_clause -> {select_clause}")

    # Assemble final SQL
    parts = []
    if not _is_null(source_system): parts.append(f"-- Source System: {source_system}")
    if not _is_null(target_schema_name) and not _is_null(target_table_name):
        parts.append(f"-- Target: {target_schema_name}.{target_table_name}")
    parts.append(f"SELECT {select_clause}")
    parts.append(f"FROM {fqn}")
    if source_join_clause: parts.append(source_join_clause)
    if source_filter_clause:
        parts.append("WHERE " + source_filter_clause)
        if debug: print(f"[DEBUG] where_clause -> {source_filter_clause}")

    final_sql = "\n".join(parts) + ";"
    if debug: print(f"[DEBUG] final_sql (source) ->\n{final_sql}\n")
    return final_sql


# -------------------------- Build TARGET-side SQL -----------------------------
def build_target_sql(table_meta, col):
    debug = _debug_on(table_meta)

    source_db_name        = table_meta.get("source_db_name")   # used for FQN unless you add target_db_name later
    target_filter_clause  = (table_meta.get("target_filter_clause") or "").strip()
    source_pk_columns     = _split_csv(table_meta.get("source_pk_columns"))

    target_schema_name    = col.get("target_schema_name")
    target_table_name     = col.get("target_table_name")
    out_name              = col.get("target_column_name") or col.get("source_column_name")

    if _is_null(target_schema_name) or _is_null(target_table_name):
        raise ValueError("target_schema_name and target_table_name are required in each column")

    target_fqn = _path(source_db_name, target_schema_name, target_table_name) if not _is_null(source_db_name) else _path(None, target_schema_name, target_table_name)
    if debug: print(f"[DEBUG] target_fqn -> {target_fqn}")

    select_parts = [f"{pk} AS {pk}" for pk in source_pk_columns]
    select_parts.append(f"{out_name} AS {out_name}")
    select_clause = ", ".join(select_parts)
    if debug: print(f"[DEBUG] select_clause (target) -> {select_clause}")

    parts = [f"SELECT {select_clause}", f"FROM {target_fqn}"]
    if target_filter_clause:
        parts.append("WHERE " + target_filter_clause)
        if debug: print(f"[DEBUG] where_clause (target) -> {target_filter_clause}")

    final_sql = "\n".join(parts) + ";"
    if debug: print(f"[DEBUG] final_sql (target) ->\n{final_sql}\n")
    return final_sql


# ----------------- Create/replace output table and insert rows ----------------
session.sql("""
    CREATE OR REPLACE TRANSIENT TABLE QA_TEMP_MAPPING_SQL (
        COLUMN_NAME       STRING,
        SQL_TEXT          STRING,
        TARGET_TABLE_SQL  STRING
    )
""").collect()

for col in columns:
    column_label = (col.get("target_column_name") or col.get("source_column_name"))
    src_sql      = build_source_sql(table_meta, col)
    tgt_sql      = build_target_sql(table_meta, col)

    column_label_esc = str(column_label).replace("'", "''")
    src_sql_esc      = src_sql.replace("'", "''")
    tgt_sql_esc      = tgt_sql.replace("'", "''")

    session.sql(f"""
        INSERT INTO QA_TEMP_MAPPING_SQL (COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL)
        VALUES ('{column_label_esc}', '{src_sql_esc}', '{tgt_sql_esc}')
    """).collect()

# Optional preview
session.sql("SELECT * FROM QA_TEMP_MAPPING_SQL ORDER BY COLUMN_NAME").show()
