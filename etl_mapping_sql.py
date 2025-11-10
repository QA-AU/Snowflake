# WORKING VERSION 

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
    "target_filter_clause": "status = 'OPEN'"
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
def _is_null(x): return x is None or (isinstance(x, str) and x.strip().lower() in ("null", ""))

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

def _apply_alias_token(expr, alias, table):
    actual = (alias if alias and not _is_null(alias) else table) + "."
    return re.sub(r'(?i)\balias\.', actual, expr)

_ALIAS = {
    "STRING":"STRING","VARCHAR":"STRING","CHAR":"STRING","CHARACTER":"STRING","NCHAR":"STRING","NVARCHAR":"STRING","TEXT":"STRING",
    "NUMBER":"NUMERIC","DECIMAL":"NUMERIC","NUMERIC":"NUMERIC","INT":"NUMERIC","INTEGER":"NUMERIC","BIGINT":"NUMERIC","SMALLINT":"NUMERIC",
    "TINYINT":"NUMERIC","FLOAT":"NUMERIC","DOUBLE":"NUMERIC","REAL":"NUMERIC",
    "BOOLEAN":"BOOL","BOOL":"BOOL","DATE":"DATE","TIME":"TIME",
    "TIMESTAMP":"TIMESTAMP_NTZ","TIMESTAMP_LTZ":"TIMESTAMP_LTZ","TIMESTAMP_TZ":"TIMESTAMP_TZ",
}
_TYPE_RE = re.compile(r"^\s*([A-Z_]+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")

def _canonical_type(t):
    if _is_null(t): return None
    raw = str(t).strip().upper()
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
    return src != tgt

def _debug_on(meta): return str(meta.get("debug_mode", "")).strip().upper() == "YES"


# -------------------------- SQL Builders ------------------------------------
def build_source_sql(meta, col):
    debug = _debug_on(meta)
    db, schema = meta["source_db_name"], meta["source_schema_name"]
    join = (meta.get("source_join_clause") or "").strip()
    flt = (meta.get("source_filter_clause") or "").strip()
    sys = meta.get("source_system")
    pks = _split_csv(meta["source_pk_columns"])

    tbl, alias = col["source_table_name"], col.get("source_alias")
    c = col["source_column_name"]
    override, lookup = col.get("source_column_override"), col.get("source_lookup_sql")
    default, sdt, tdt = col.get("source_default_value"), col.get("source_data_type"), col.get("target_data_type")
    tgt_schema, tgt_table = col["target_schema_name"], col["target_table_name"]
    out_name = col.get("target_column_name") or c

    fqn = _path(db, schema, tbl) + (f" {alias}" if alias else "")
    if debug: print(f"[DEBUG] fqn -> {fqn}")

    # Expression precedence
    if not _is_null(override):
        expr = _apply_alias_token(override.strip(), alias, tbl)
        if debug: print(f"[DEBUG] override_expr -> {expr}")
    elif not _is_null(lookup):
        expr = _apply_alias_token(lookup.strip(), alias, tbl)
        if debug: print(f"[DEBUG] lookup_sql -> {expr}")
    elif alias:
        expr = f"{alias}.{c}"
    else:
        expr = c

    if not _is_null(default):
        expr = f"COALESCE({expr}, {_q_lit(default)})"
        if debug: print(f"[DEBUG] default_value -> {default}")

    if _needs_cast(sdt, tdt):
        expr = f"CAST({expr} AS {tdt.strip()})"
        if debug: print(f"[DEBUG] cast_applied -> AS {tdt.strip()}")

    sel = [f"{alias}.{pk}" if alias else pk + f" AS {pk}" for pk in pks]
    sel = [f"{alias}.{pk} AS {pk}" if alias else f"{pk} AS {pk}" for pk in pks]
    sel.append(f"{expr} AS {out_name}")
    select_clause = ", ".join(sel)
    if debug: print(f"[DEBUG] select_clause -> {select_clause}")

    sql = []
    if sys: sql.append(f"-- Source System: {sys}")
    sql.append(f"-- Target: {tgt_schema}.{tgt_table}")
    sql.append(f"SELECT {select_clause}")
    sql.append(f"FROM {fqn}")
    if join: sql.append(join)
    if flt:
        sql.append("WHERE " + flt)
        if debug: print(f"[DEBUG] where_clause -> {flt}")
    return "\n".join(sql) + ";"


def build_target_sql(meta, col):
    debug = _debug_on(meta)
    db = meta["source_db_name"]
    tflt = (meta.get("target_filter_clause") or "").strip()
    pks = _split_csv(meta["source_pk_columns"])
    tgt_schema, tgt_table = col["target_schema_name"], col["target_table_name"]
    out_name = col.get("target_column_name") or col["source_column_name"]

    fqn = _path(db, tgt_schema, tgt_table)
    if debug: print(f"[DEBUG] target_fqn -> {fqn}")

    sel = [f"{pk} AS {pk}" for pk in pks] + [f"{out_name} AS {out_name}"]
    select_clause = ", ".join(sel)

    sql = [f"SELECT {select_clause}", f"FROM {fqn}"]
    if tflt:
        sql.append("WHERE " + tflt)
        if debug: print(f"[DEBUG] where_clause (target) -> {tflt}")
    return "\n".join(sql) + ";"


# --------------------------- Main Entry -------------------------------------
def main(session: Session):
    """Main Snowpark entrypoint for generating mapping SQL."""
    session.sql("""
        CREATE OR REPLACE TRANSIENT TABLE QA_TEMP_MAPPING_SQL (
            COLUMN_NAME STRING,
            SQL_TEXT STRING,
            TARGET_TABLE_SQL STRING
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
            INSERT INTO QA_TEMP_MAPPING_SQL (COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL)
            VALUES ('{col_esc}', '{src_esc}', '{tgt_esc}')
        """).collect()

    result = session.sql("SELECT COUNT(*) AS CNT FROM QA_TEMP_MAPPING_SQL").collect()[0]["CNT"]
    print(f"✅ Generated {result} SQL mappings.")
    return f"Generated {result} SQL mappings."

# --------------------------- Run interactively ------------------------------
print(main(session))
session.sql("SELECT * FROM QA_TEMP_MAPPING_SQL ORDER BY COLUMN_NAME").show()
