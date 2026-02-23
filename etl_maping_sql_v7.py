from snowflake.snowpark import Session
import re

# ===================== Configuration =========================================
TABLE_META = {
    "debug_mode": "YES",
    "source_system": "ERP",
    "source_db_name": "SALES_DB",
    "source_schema_name": "PUBLIC",
    # You may put fully qualified PKs here, including AS aliases:
    # "SAP_T1.P1 AS P1, SAP_T2.P2 AS P2"
    "source_pk_columns": "ORDER_ID, LINE_NO",
    "source_join_clause": "LEFT JOIN LKP.CUST c ON c.id = o.cust_id",
    "source_filter_clause": "o.status = 'OPEN'",
    # left unresolved here – replaced at execution time
    "date_filter": "order_date between XX.START_DT and XX.END_DT",
    "date_filter_token": "order_date",
    "target_filter_clause": "status = 'OPEN'",
    # Where to store generated SQL
    "qa_db_name": None,
    "qa_schema_name": "STG",
    "qa_table_name": "QA_TEMP_MAPPING_SQL",
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
    return [p.strip() for p in s.split(",") if p and p.strip()] if s else []


def _apply_alias_token(expr, alias, table):
    actual = (alias if alias else table) + "."
    return re.sub(r"(?i)\balias\.", actual, expr)


def _combine_filters(*parts):
    toks = [p.strip() for p in parts if p and str(p).strip()]
    if not toks:
        return ""
    return " AND ".join([f"({t})" for t in toks])


def _debug_on(meta):
    return str(meta.get("debug_mode", "")).upper() == "YES"


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
    if not expr:
        return expr
    m = re.match(r"^(.*)\s+AS\s+([A-Za-z_][A-Za-z0-9_$]*)$", expr.strip(), flags=re.I)
    if m:
        return m.group(1)
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
    if order_date_value is None:
        return sql
    lit = "'" + str(order_date_value).replace("'", "''") + "'"
    return re.sub(r"\border_date\b", lit, sql)


# ------------------------ SQL BUILDERS --------------------------------------
def build_source_sql(meta, col):
    debug = _debug_on(meta)
    db, schema = meta["source_db_name"], meta["source_schema_name"]
    join = meta.get("source_join_clause") or ""
    src_flt = meta.get("source_filter_clause") or ""
    pk_list = _split_csv(meta["source_pk_columns"])

    tbl = col["source_table_name"]
    alias = col.get("source_alias")
    c = col["source_column_name"]
    override = col.get("source_column_override")
    lookup = col.get("source_lookup_sql")
    default = col.get("source_default_value")
    sdt = col.get("source_data_type")
    tdt = col.get("target_data_type")
    tgt_schema = col["target_schema_name"]
    tgt_table = col["target_table_name"]
    out_name = col["target_column_name"]

    fqn = _path(db, schema, tbl) + (f" {alias}" if alias else "")

    # determine base expr
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

    if tdt and sdt:
        # cast only if needed
        # (safe: Snowflake ignores cast if types already match)
        expr = f"CAST({expr} AS {tdt})"

    # Build SELECT list (PKs + data column)
    sel = []
    for pk_raw in pk_list:
        pk_expr, pk_alias = _parse_pk_expr(pk_raw)
        sel.append(f"{pk_expr} AS {pk_alias}")

    sel.append(f"{expr} AS {out_name}")

    date_flt = _resolve_date_filter(meta)
    where_combined = _combine_filters(src_flt, date_flt)

    sql = []
    sql.append(f"-- Target: {tgt_schema}.{tgt_table}")
    sql.append(f"SELECT {', '.join(sel)}")
    sql.append(f"FROM {fqn}")
    if join:
        sql.append(join)
    if where_combined:
        sql.append(f"WHERE {where_combined}")
    return "\n".join(sql) + ";"


def build_target_sql(meta, col):
    db = meta["source_db_name"]
    tgt_flt = meta.get("target_filter_clause") or ""
    pk_list = _split_csv(meta["source_pk_columns"])

    tgt_schema = col["target_schema_name"]
    tgt_table = col["target_table_name"]
    out_name = col["target_column_name"]

    fqn = _path(db, tgt_schema, tgt_table)

    pk_sel = []
    for pk_raw in pk_list:
        _, pk_alias = _parse_pk_expr(pk_raw)
        pk_sel.append(f"{pk_alias} AS {pk_alias}")

    select_clause = ", ".join(pk_sel + [f"{out_name} AS {out_name}"])
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
    """Creates/refreshes STG.QA_TEMP_MAPPING_SQL and inserts SQL_TEXT + TARGET_TABLE_SQL."""

    qa_table = _qa_table_fqn()

    session.sql(f"""
        CREATE OR REPLACE TRANSIENT TABLE {qa_table} (
            ROW_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            COLUMN_NAME STRING,
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

    for col in COLUMNS:
        label = col["target_column_name"]
        src_sql = build_source_sql(TABLE_META, col)
        tgt_sql = build_target_sql(TABLE_META, col)

        session.sql(f"""
            INSERT INTO {qa_table} (COLUMN_NAME, SQL_TEXT, TARGET_TABLE_SQL)
            VALUES ('{label.replace("'", "''")}',
                    '{src_sql.replace("'", "''")}',
                    '{tgt_sql.replace("'", "''")}')
        """).collect()

    return session.sql(f"SELECT * FROM {qa_table} ORDER BY ROW_ID")


# ------------------------------ VALIDATION ----------------------------------
def prepare_validation_sqls(session: Session, table_fqn: str = None):
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
            "SELECT 'SRC' AS SIDE, COUNT(*) AS CNT FROM ("
            + s_inner
            + ") SRC_T\nUNION ALL\nSELECT 'TGT' AS SIDE, COUNT(*) AS CNT FROM ("
            + t_inner
            + ") TGT_T"
        )

        diff_sql = (
            "SELECT COUNT(*) AS DIFF_CNT FROM (\n("
            + s_inner
            + ")\nMINUS\n("
            + t_inner
            + ")\n) D"
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

        # execute COUNT
        try:
            cnt_rows = session.sql(cexec).collect()
            t = {"SRC": 0, "TGT": 0}
            for cr in cnt_rows:
                t[cr["SIDE"]] = int(cr["CNT"])
            counts = t
        except Exception as e:
            count_err = str(e)[:4000].replace("'", "''")

        # execute DIFF
        try:
            d = session.sql(dexec).collect()
            diff_val = int(d[0]["DIFF_CNT"]) if d else 0
        except Exception as e:
            diff_err = str(e)[:4000].replace("'", "''")

        counts_json = (
            f"""PARSE_JSON('{{"SRC": {counts["SRC"]}, "TGT": {counts["TGT"]}}}')"""
            if count_err is None
            else "NULL"
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
    qa = _qa_table_fqn()

    prepare_validation_sqls(session, qa)
    run_validation_sqls(session, qa, order_date_value)

    return session.sql(f"""
        SELECT ROW_ID, COLUMN_NAME, COUNT_RESULT_JSON, DIFF_RESULT, COUNT_ERROR, DIFF_ERROR
        FROM {qa}
        ORDER BY ROW_ID
    """)


def main(session: Session):
    """
    Do NOT auto-execute. User runs main(session) manually.
    """
    order_date_value = "2024-06-30"
    main_generate_sql(session)
    return main_validate(session, order_date_value)
