from snowflake.snowpark import Session
import re

# ===================== Configuration =========================================
TABLE_META = {
    "debug_mode": "YES",

    "source_system": "ERP",
    "source_db_name": "SALES_DB",
    "source_schema_name": "PUBLIC",

    # Can be:
    #   "PK1, PK2"
    #   "T1.PK1, T2.PK2"
    #   "T1.PK1 AS PK1, T2.PK2 AS PK2"
    "source_pk_columns": "ORDER_ID, LINE_NO",

    "source_join_clause": "LEFT JOIN LKP.CUST c ON c.id = o.cust_id",
    "source_filter_clause": "o.status = 'OPEN'",

    # left unresolved – replaced at execution time
    "date_filter": "order_date between XX.START_DT and XX.END_DT",
    "date_filter_token": "order_date",

    "target_filter_clause": "status = 'OPEN'",

    # Where to store generated SQL
    "qa_db_name": None,
    "qa_schema_name": "STG",
    "qa_table_name": "QA_TEMP_MAPPING_SQL"
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
    if not _is_null(db): parts.append(db.strip())
    if not _is_null(schema): parts.append(schema.strip())
    parts.append(table.strip())
    return ".".join(parts)

def _q_lit(v):
    if _is_null(v): return "NULL"
    if isinstance(v, (int, float)): return str(v)
    return "'" + str(v).replace("'", "''") + "'"

def _split_csv(s):
    return [p.strip() for p in s.split(",") if p and p.strip()] if s else []

def _apply_alias_token(expr, alias, table):
    actual = (alias if alias else table) + "."
    return re.sub(r'(?i)\balias\.', actual, expr)

def _combine_filters(*parts):
    toks = [p.strip() for p in parts if p and str(p).strip()]
    if not toks: return ""
    return " AND ".join([f"({t})" for t in toks])

def _debug_on(meta):
    return str(meta.get("debug_mode","")).upper() == "YES"

def _resolve_date_filter(meta):
    return (meta.get("date_filter") or "").strip()

def _qa_table_fqn(meta=None):
    meta = meta or TABLE_META
    db = meta.get("qa_db_name") or meta.get("source_db_name")
    schema = meta.get("qa_schema_name")
    table = meta.get("qa_table_name")
    return _path(db, schema, table)

# --- alias stripping to avoid "expr AS X AS X"
def _strip_final_alias(expr: str):
    if not expr: return expr
    m = re.match(r"^(.*)\s+AS\s+([A-Za-z_][A-Za-z0-9_$]*)$", expr.strip(), flags=re.I)
    if m: return m.group(1)
    return expr

# --- PK parser (supports FQ, AS)
def _parse_pk_expr(pk_raw: str):
    txt = pk_raw.strip()
    m = re.match(r"^(.*)\s+AS\s+([A-Za-z_][A-Za-z0-9_$]*)$", txt, flags=re.I)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    expr = txt
    alias = txt.split(".")[-1].strip()
    return expr, alias

# --- runtime injection of order_date
def _inject_order_date(sql, order_date_value):
    if order_date_value is None: return sql
    lit = "'" + str(order_date_value).replace("'", "''") + "'"
    return re.sub(r"\border_date\b", lit, sql)


# ------------- NEW: per-column expression builder (no SELECT wrapper) -------
def _build_column_expr(meta, col):
    """
    Build the source expression for a single column (no SELECT),
    applying override/lookup/default/cast, and stripping any
    embedded 'AS alias' from the base expression.
    """
    db, schema = meta["source_db_name"], meta["source_schema_name"]
    debug = _debug_on(meta)

    tbl = col["source_table_name"]
    alias = col.get("source_alias")
    c = col["source_column_name"]
    override = col.get("source_column_override")
    lookup = col.get("source_lookup_sql")
    default = col.get("source_default_value")
    sdt = col.get("source_data_type")
    tdt = col.get("target_data_type")
    out_name = col["target_column_name"]

    # precedence: override > lookup > alias.column > column
    if not _is_null(override):
        expr = _apply_alias_token(override.strip(), alias, tbl)
    elif not _is_null(lookup):
        expr = _apply_alias_token(lookup.strip(), alias, tbl)
    elif alias:
        expr = f"{alias}.{c}"
    else:
        expr = c

    expr = _strip_final_alias(expr)

    if not _is_null(default):
        expr = f"COALESCE({expr}, {_q_lit(default)})"

    if sdt and tdt:
        expr = f"CAST({expr} AS {tdt})"

    if debug:
        print(f"[DEBUG] col={out_name} expr={expr}")

    return expr, out_name


# ------------------------ SQL BUILDERS (grouped) ----------------------------
def build_source_sql_for_group(meta, cols):
    """
    Build a single SELECT for all PKs + all mapped columns in this group.
    Assumes all columns in `cols` are for the same target table.
    Uses the first column's source_table_name/alias as the FROM base.
    """
    debug = _debug_on(meta)
    db, schema = meta["source_db_name"], meta["source_schema_name"]
    join = (meta.get("source_join_clause") or "")
    src_flt = (meta.get("source_filter_clause") or "")
    pk_list = _split_csv(meta["source_pk_columns"])

    # base table/alias from first column in the group
    base_col = cols[0]
    base_tbl = base_col["source_table_name"]
    base_alias = base_col.get("source_alias")

    tgt_schema = base_col["target_schema_name"]
    tgt_table = base_col["target_table_name"]

    fqn = _path(db, schema, base_tbl) + (f" {base_alias}" if base_alias else "")

    if debug:
        print(f"[DEBUG] SOURCE_GROUP base_fqn={fqn} cols={[c['target_column_name'] for c in cols]}")

    # PKs
    sel_parts = []
    for pk_raw in pk_list:
        pk_expr, pk_alias = _parse_pk_expr(pk_raw)
        sel_parts.append(f"{pk_expr} AS {pk_alias}")

    # data columns
    for col in cols:
        expr, out_name = _build_column_expr(meta, col)
        sel_parts.append(f"{expr} AS {out_name}")

    select_clause = ", ".join(sel_parts)

    date_flt = _resolve_date_filter(meta)
    where_combined = _combine_filters(src_flt, date_flt)

    sql = []
    sql.append(f"-- Target: {tgt_schema}.{tgt_table}")
    sql.append(f"SELECT {select_clause}")
    sql.append(f"FROM {fqn}")
    if join:
        sql.append(join)
    if where_combined:
        sql.append(f"WHERE {where_combined}")
    return "\n".join(sql) + ";"


def build_target_sql_for_group(meta, cols):
    """
    Build a single SELECT from the target table with all PKs + all mapped columns.
    Uses the first column's target_schema_name/target_table_name as the base.
    Assumes target table already has PK columns named by the PK aliases and
    mapped columns named as target_column_name.
    """
    debug = _debug_on(meta)
    db = meta["source_db_name"]
    tgt_flt = (meta.get("target_filter_clause") or "")
    pk_list = _split_csv(meta["source_pk_columns"])

    base_col = cols[0]
    tgt_schema = base_col["target_schema_name"]
    tgt_table = base_col["target_table_name"]
    fqn = _path(db, tgt_schema, tgt_table)

    if debug:
        print(f"[DEBUG] TARGET_GROUP fqn={fqn} cols={[c['target_column_name'] for c in cols]}")

    # PKs: use the alias side
    pk_sel = []
    for pk_raw in pk_list:
        _, pk_alias = _parse_pk_expr(pk_raw)
        pk_sel.append(f"{pk_alias} AS {pk_alias}")

    # data columns: just select by target name
    data_sel = [f"{c['target_column_name']} AS {c['target_column_name']}" for c in cols]

    select_clause = ", ".join(pk_sel + data_sel)

    date_flt = _resolve_date_filter(meta)
    where_combined = _combine_filters(tgt_flt, date_flt)

    sql = []
    sql.append(f"SELECT {select_clause}")
    sql.append(f"FROM {fqn}")
    if where_combined:
        sql.append(f"WHERE {where_combined}")
    return "\n".join(sql) + ";"


# ----------------------------- GENERATOR ------------------------------------
def main_generate_sql(session: Session):
    """
    Creates/refreshes STG.QA_TEMP_MAPPING_SQL and inserts *one row per target table*:
      - SQL_TEXT         : full source SELECT (PKs + all mapped columns)
      - TARGET_TABLE_SQL : full target SELECT (PKs + all mapped columns)
    """
    from collections import defaultdict

    qa_table = _qa_table_fqn()

    # Create QA table
    session.sql(f"""
        CREATE OR REPLACE TRANSIENT TABLE {qa_table} (
            ROW_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            COLUMN_NAME STRING,           -- here used as TARGET_TABLE_ID (e.g. 'CORE.ORDERS')
            SQL_TEXT STRING,
            TARGET_TABLE_SQL STRING,
            COUNT_SQL STRING,
            DIFF_SQL STRING,
            COUNT_RESULT_JSON VARIANT,
            DIFF_RESULT NUMBER,
            COUNT_ERROR STRING,
            DIFF_ERROR STRING
        )
    """).collect()

    # Group columns by (target_schema, target_table)
    groups = defaultdict(list)
    for col in COLUMNS:
        key = (col["target_schema_name"], col["target_table_name"])
        groups[key].append(col)

    # Build one SQL per group
    for (tgt_schema, tgt_table), cols in groups.items():
        src_sql = build_source_sql_for_group(TABLE_META, cols)
        tgt_sql = build_target_sql_for_group(TABLE_META, cols)

        target_id = f"{tgt_schema}.{tgt_table}".replace("'", "''")
        src_esc = src_sql.replace("'", "''")
        tgt_esc = tgt_sql.replace("'", "''")

        session.sql(f"""
            INSERT INTO {qa_table} (COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL)
            VALUES ('{target_id}', '{src_esc}', '{tgt_esc}')
        """).collect()

    return session.sql(f"SELECT * FROM {qa_table} ORDER BY ROW_ID")


# ------------------------------ VALIDATION ----------------------------------
def prepare_validation_sqls(session: Session, table_fqn: str = None):
    """
    Wrap each SQL_TEXT/TARGET_TABLE_SQL row into COUNT_SQL and DIFF_SQL.
    One row == one full-table compare (PKs + all mapped columns).
    """
    table_fqn = table_fqn or _qa_table_fqn()

    rows = session.sql(f"""
        SELECT ROW_ID, SQL_TEXT, TARGET_TABLE_SQL
        FROM {table_fqn}
        ORDER BY ROW_ID
    """).collect()

    for r in rows:
        row_id = r["ROW_ID"]
        s = r["SQL_TEXT"]
        t = r["TARGET_TABLE_SQL"]
        if not s or not t:
            continue

        s_inner = s.rstrip(" ;\n\t")
        t_inner = t.rstrip(" ;\n\t")

        count_sql = (
            "SELECT 'SRC' AS SIDE, COUNT(*) AS CNT FROM (" + s_inner +
            ") SRC_T\nUNION ALL\nSELECT 'TGT' AS SIDE, COUNT(*) AS CNT FROM (" +
            t_inner + ") TGT_T"
        )

        diff_sql = (
            "SELECT COUNT(*) AS DIFF_CNT FROM (\n(" + s_inner +
            ")\nMINUS\n(" + t_inner + ")\n) D"
        )

        session.sql(f"""
            UPDATE {table_fqn}
            SET COUNT_SQL='{count_sql.replace("'", "''")}',
                DIFF_SQL='{diff_sql.replace("'", "''")}',
                COUNT_RESULT_JSON=NULL,
                DIFF_RESULT=NULL,
                COUNT_ERROR=NULL,
                DIFF_ERROR=NULL
            WHERE ROW_ID={row_id}
        """).collect()


def run_validation_sqls(session: Session, table_fqn: str = None, order_date_value=None):
    """
    Execute COUNT_SQL and DIFF_SQL for each row, substituting order_date if given.
    """
    table_fqn = table_fqn or _qa_table_fqn()

    rows = session.sql(f"""
        SELECT ROW_ID, COUNT_SQL, DIFF_SQL
        FROM {table_fqn}
        ORDER BY ROW_ID
    """).collect()

    for r in rows:
        row_id = r["ROW_ID"]
        csql = r["COUNT_SQL"]
        dsql = r["DIFF_SQL"]
        if not csql or not dsql:
            continue

        cexec = _inject_order_date(csql, order_date_value)
        dexec = _inject_order_date(dsql, order_date_value)

        count_err = diff_err = None
        diff_val = None
        counts = {"SRC": None, "TGT": None}

        # COUNT
        try:
            cnt_rows = session.sql(cexec).collect()
            tmp = {"SRC": 0, "TGT": 0}
            for cr in cnt_rows:
                side = str(cr["SIDE"]).upper()
                val = int(cr["CNT"])
                if side in tmp:
                    tmp[side] = val
            counts = tmp
        except Exception as e:
            count_err = str(e)[:4000].replace("'", "''")

        # DIFF
        try:
            drows = session.sql(dexec).collect()
            diff_val = int(drows[0]["DIFF_CNT"]) if drows else 0
        except Exception as e:
            diff_err = str(e)[:4000].replace("'", "''")

        counts_json = (
            f"""PARSE_JSON('{{"SRC": {counts["SRC"]}, "TGT": {counts["TGT"]}}}')"""
            if count_err is None else "NULL"
        )
        diff_expr = "NULL" if diff_val is None else str(diff_val)
        c_err = "NULL" if count_err is None else f"'{count_err}'"
        d_err = "NULL" if diff_err is None else f"'{diff_err}'"

        session.sql(f"""
            UPDATE {table_fqn}
            SET COUNT_RESULT_JSON={counts_json},
                DIFF_RESULT={diff_expr},
                COUNT_ERROR={c_err},
                DIFF_ERROR={d_err}
            WHERE ROW_ID={row_id}
        """).collect()


# --------------------------- ORCHESTRATOR -----------------------------------
def main_validate(session: Session, order_date_value=None):
    """
    Prepare + run validation for all table-level mappings.
    """
    qa = _qa_table_fqn()
    prepare_validation_sqls(session, qa)
    run_validation_sqls(session, qa, order_date_value)

    return session.sql(f"""
        SELECT ROW_ID,
               COLUMN_NAME,          -- used as TARGET_TABLE_ID (schema.table)
               COUNT_RESULT_JSON,
               DIFF_RESULT,
               COUNT_ERROR,
               DIFF_ERROR
        FROM {qa}
        ORDER BY ROW_ID
    """)


def main(session: Session):
    """
    Full pipeline:
      - Generate one row per target table with full SELECTs
      - Prepare COUNT/DIFF SQLs
      - Execute them
      - Return summary
    """
    order_date_value = "2024-06-30"  # change as needed
    main_generate_sql(session)
    return main_validate(session, order_date_value)
