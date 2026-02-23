from snowflake.snowpark import Session
import json
import time
from copy import deepcopy
from typing import Optional, Dict, Any, List

# =====================================================================
# CONFIG: Default DB + per-table metadata
# =====================================================================

PARENT_DB = "SESAME"  # used as default if you want

TABLE_TEST_META: Dict[str, Dict[str, Any]] = {
    "CORE.ORDERS": {
        "debug_mode": "NO",
        "table": {
            "schema": "CORE",
            "name": "ORDERS",
            "pk_columns": ["ORDER_ID", "LINE_NO"],
            "business_date_column": "BUSINESS_DATE",
            "date_columns": ["ORDER_DATE", "SHIP_DATE"],
            "timestamp_columns": ["CREATED_AT", "UPDATED_AT"],
            "trim_columns": ["CUSTOMER_NAME", "ADDRESS"],
            "clean_columns": ["NOTES", "COMMENTS"],
            "scd": {
                "natural_key_columns": ["CUSTOMER_ID"],
                "start_date_column": "VALID_FROM",
                "end_date_column": "VALID_TO",
                "current_flag_column": "IS_CURRENT",
                "open_end_value": None,
            },
            "fk_relations": [
                {
                    "fk_name": "FK_ORDERS_CUSTOMER",
                    "child_column": "CUSTOMER_ID",
                    "parent_schema": "CORE",
                    "parent_table": "CUSTOMERS",
                    "parent_key_column": "CUSTOMER_ID",
                }
            ],
            "extra_filter": "STATUS = 'OPEN'",
        },
        "tests_to_run": [
            "BUSINESS_DATE_MATCH",
            "NON_ZERO_COUNT_FOR_BUSINESS_DATE",
            "DUPLICATE_PKS",
            "PK_NOT_NULL",
            "DATE_COLS_NOT_NULL",
            "TIMESTAMP_COLS_NOT_NULL",
            "TRIMMED_COLS",
            "CLEANED_COLS",
            "SINGLE_OPEN_RECORD",
            "FK_NO_ORPHANS",
        ],
    }
}

# =====================================================================
# Helpers
# =====================================================================


def _ensure_results_table(session: Session):
    session.sql("""
        CREATE TEMP TABLE IF NOT EXISTS QA_SHAKEDOWN_RESULTS (
            RESULT_ID      NUMBER IDENTITY,
            RUN_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            RUN_NAME       STRING,
            TABLE_DB       STRING,
            TABLE_SCHEMA   STRING,
            TABLE_NAME     STRING,
            BUSINESS_DATE  STRING,
            TEST_NAME      STRING,
            RESOLVED_SQL   STRING,
            METRICS        VARIANT,
            PASS_FLAG      BOOLEAN,
            ERROR          STRING,
            DURATION_MS    NUMBER
        )
    """).collect()


def _escape_sql_literal(s: Optional[str]) -> str:
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _metrics_to_expr(metrics: Optional[Dict[str, Any]]) -> str:
    if metrics is None:
        return "NULL"
    js = json.dumps(metrics)
    js = js.replace("'", "''")
    return f"PARSE_JSON('{js}')"


def _bool_to_expr(b: Optional[bool]) -> str:
    if b is None:
        return "NULL"
    return "TRUE" if b else "FALSE"


def _num_to_expr(x: Optional[Any]) -> str:
    if x is None:
        return "NULL"
    return str(x)


def _fqn(parent_db: str, schema: str, table: str) -> str:
    return f"{parent_db}.{schema}.{table}"


def _should_apply_business_date(business_date_value: Optional[str]) -> bool:
    """
    BD rule:
      - If business_date_value == '1900-01-01' -> DO NOT apply BD filter anywhere.
      - If business_date_value is None        -> no BD filter AND BD tests will error.
      - Else                                  -> apply BD filter where relevant.
    """
    return business_date_value is not None and business_date_value != "1900-01-01"


def _build_where_clause(
    extra_filter: Optional[str],
    date_filter: Optional[str],
    business_date_column: Optional[str],
    business_date_value: Optional[str],
    use_bd: bool,
) -> str:
    clauses: List[str] = []
    if use_bd and business_date_column:
        clauses.append(f"{business_date_column} = '{business_date_value}'")
    if extra_filter:
        clauses.append(extra_filter)
    if date_filter:
        clauses.append(date_filter)
    if not clauses:
        return ""
    wrapped = [f"({c})" for c in clauses]
    return "WHERE " + " AND ".join(wrapped)


def _run_sql_with_timing(session: Session, sql: str, debug: bool, test_name: str):
    if debug:
        print(f"[DEBUG][{test_name}] Executing SQL:\n{sql}\n")
    start = time.time()
    rows = session.sql(sql).collect()
    dur_ms = int((time.time() - start) * 1000)
    if debug:
        print(f"[DEBUG][{test_name}] Completed in {dur_ms} ms, rows={len(rows)}")
    return rows, dur_ms


def _insert_result_row(
    session: Session,
    run_name: str,
    table_db: str,
    schema: str,
    table: str,
    business_date_value: Optional[str],
    test_name: str,
    resolved_sql: Optional[str],
    metrics: Optional[Dict[str, Any]],
    pass_flag: Optional[bool],
    error: Optional[str],
    duration_ms: Optional[int],
    debug: bool,
):
    if debug:
        print(
            f"[DEBUG][{test_name}] Inserting result: pass={pass_flag}, error={error}, metrics={metrics}"
        )

    run_name_esc = _escape_sql_literal(run_name)
    db_esc = _escape_sql_literal(table_db)
    schema_esc = _escape_sql_literal(schema)
    table_esc = _escape_sql_literal(table)
    bd_esc = _escape_sql_literal(business_date_value) if business_date_value else "NULL"
    test_esc = _escape_sql_literal(test_name)
    sql_esc = _escape_sql_literal(resolved_sql) if resolved_sql else "NULL"
    metrics_expr = _metrics_to_expr(metrics)
    pass_expr = _bool_to_expr(pass_flag)
    err_esc = _escape_sql_literal(error) if error else "NULL"
    dur_expr = _num_to_expr(duration_ms)

    session.sql(f"""
        INSERT INTO QA_SHAKEDOWN_RESULTS
        (RUN_NAME, TABLE_DB, TABLE_SCHEMA, TABLE_NAME, BUSINESS_DATE, TEST_NAME,
         RESOLVED_SQL, METRICS, PASS_FLAG, ERROR, DURATION_MS)
        VALUES (
            {run_name_esc},
            {db_esc},
            {schema_esc},
            {table_esc},
            {bd_esc},
            {test_esc},
            {sql_esc},
            {metrics_expr},
            {pass_expr},
            {err_esc},
            {dur_expr}
        )
    """).collect()


# =====================================================================
# Individual tests (all have debug flag)
# =====================================================================


def _test_business_date_match(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "BUSINESS_DATE_MATCH"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    bd_col = tbl_cfg.get("business_date_column")
    extra = tbl_cfg.get("extra_filter")

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table} with BD={business_date_value}, use_bd={use_bd}"
        )

    if business_date_value is None:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "business_date_value is required for BUSINESS_DATE_MATCH",
            None,
            debug,
        )
        return

    where_clause = _build_where_clause(
        extra, date_filter, bd_col, business_date_value, use_bd=use_bd
    )
    sql = f"SELECT COUNT(*) AS CNT FROM {fqn} {where_clause}"

    try:
        rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
        cnt = rows[0]["CNT"] if rows else 0
        passed = cnt > 0
        metrics = {"CNT": cnt}
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            metrics,
            passed,
            None,
            dur,
            debug,
        )
    except Exception as e:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


def _test_non_zero_count_for_bd(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "NON_ZERO_COUNT_FOR_BUSINESS_DATE"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    bd_col = tbl_cfg.get("business_date_column")
    extra = tbl_cfg.get("extra_filter")

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table} with BD={business_date_value}, use_bd={use_bd}"
        )

    if business_date_value is None:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "business_date_value is required for NON_ZERO_COUNT_FOR_BUSINESS_DATE",
            None,
            debug,
        )
        return

    where_clause = _build_where_clause(
        extra, date_filter, bd_col, business_date_value, use_bd=use_bd
    )
    sql = f"SELECT COUNT(*) AS CNT FROM {fqn} {where_clause}"

    try:
        rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
        cnt = rows[0]["CNT"] if rows else 0
        passed = cnt > 0
        metrics = {"CNT": cnt}
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            metrics,
            passed,
            None,
            dur,
            debug,
        )
    except Exception as e:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


def _test_duplicate_pks(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "DUPLICATE_PKS"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    pk_cols = tbl_cfg.get("pk_columns") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, pk_cols={pk_cols}"
        )

    if not pk_cols:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "pk_columns not specified for DUPLICATE_PKS",
            None,
            debug,
        )
        return

    pk_list = ", ".join(pk_cols)
    where_clause = _build_where_clause(
        extra, date_filter, bd_col, business_date_value, use_bd=use_bd
    )

    sql = f"""
        SELECT COUNT(*) AS DUP_CNT
        FROM (
          SELECT {pk_list}, COUNT(*) AS C
          FROM {fqn}
          {where_clause}
          GROUP BY {pk_list}
          HAVING COUNT(*) > 1
        ) D
    """

    try:
        rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
        dup_cnt = rows[0]["DUP_CNT"] if rows else 0
        passed = dup_cnt == 0
        metrics = {"DUP_CNT": dup_cnt}
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            metrics,
            passed,
            None,
            dur,
            debug,
        )
    except Exception as e:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


def _test_pk_not_null(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "PK_NOT_NULL"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    pk_cols = tbl_cfg.get("pk_columns") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, pk_cols={pk_cols}"
        )

    if not pk_cols:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "pk_columns not specified for PK_NOT_NULL",
            None,
            debug,
        )
        return

    null_pred = " OR ".join([f"{c} IS NULL" for c in pk_cols])
    base_where = _build_where_clause(
        extra, date_filter, bd_col, business_date_value, use_bd=use_bd
    )

    if base_where:
        where_clause = base_where + f" AND ({null_pred})"
    else:
        where_clause = f"WHERE ({null_pred})"

    sql = f"SELECT COUNT(*) AS NULL_PK_CNT FROM {fqn} {where_clause}"

    try:
        rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
        cnt = rows[0]["NULL_PK_CNT"] if rows else 0
        passed = cnt == 0
        metrics = {"NULL_PK_CNT": cnt}
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            metrics,
            passed,
            None,
            dur,
            debug,
        )
    except Exception as e:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


def _test_date_cols_not_null(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "DATE_COLS_NOT_NULL"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    date_cols = tbl_cfg.get("date_columns") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, date_cols={date_cols}"
        )

    if not date_cols:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "date_columns not specified",
            None,
            debug,
        )
        return

    total_nulls = 0
    sql_list: List[str] = []
    error = None
    total_dur = 0

    for col in date_cols:
        where_clause = _build_where_clause(
            extra, date_filter, bd_col, business_date_value, use_bd=use_bd
        )
        if where_clause:
            where_clause = where_clause + f" AND {col} IS NULL"
        else:
            where_clause = f"WHERE {col} IS NULL"

        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where_clause}"
        sql_list.append(sql)
        try:
            rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
            total_dur += dur
            cnt = rows[0]["NULL_CNT"] if rows else 0
            total_nulls += cnt
        except Exception as e:
            error = f"Error on column {col}: {str(e)[:3000]}"
            break

    combined_sql = ";\n".join(sql_list)
    passed = (error is None) and (total_nulls == 0)
    metrics = None if error else {"TOTAL_NULLS": total_nulls, "COLUMNS": date_cols}

    _insert_result_row(
        session,
        run_name,
        parent_db,
        schema,
        table,
        business_date_value,
        test_name,
        combined_sql,
        metrics,
        passed,
        error,
        total_dur if error is None else None,
        debug,
    )


def _test_timestamp_cols_not_null(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "TIMESTAMP_COLS_NOT_NULL"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    ts_cols = tbl_cfg.get("timestamp_columns") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, ts_cols={ts_cols}"
        )

    if not ts_cols:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "timestamp_columns not specified",
            None,
            debug,
        )
        return

    total_nulls = 0
    sql_list: List[str] = []
    error = None
    total_dur = 0

    for col in ts_cols:
        where_clause = _build_where_clause(
            extra, date_filter, bd_col, business_date_value, use_bd=use_bd
        )
        if where_clause:
            where_clause = where_clause + f" AND {col} IS NULL"
        else:
            where_clause = f"WHERE {col} IS NULL"

        sql = f"SELECT COUNT(*) AS NULL_CNT FROM {fqn} {where_clause}"
        sql_list.append(sql)
        try:
            rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
            total_dur += dur
            cnt = rows[0]["NULL_CNT"] if rows else 0
            total_nulls += cnt
        except Exception as e:
            error = f"Error on column {col}: {str(e)[:3000]}"
            break

    combined_sql = ";\n".join(sql_list)
    passed = (error is None) and (total_nulls == 0)
    metrics = None if error else {"TOTAL_NULLS": total_nulls, "COLUMNS": ts_cols}

    _insert_result_row(
        session,
        run_name,
        parent_db,
        schema,
        table,
        business_date_value,
        test_name,
        combined_sql,
        metrics,
        passed,
        error,
        total_dur if error is None else None,
        debug,
    )


def _test_trimmed_cols(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "TRIMMED_COLS"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    trim_cols = tbl_cfg.get("trim_columns") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, trim_cols={trim_cols}"
        )

    if not trim_cols:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "trim_columns not specified",
            None,
            debug,
        )
        return

    total_viol = 0
    sql_list: List[str] = []
    error = None
    total_dur = 0

    for col in trim_cols:
        where_clause = _build_where_clause(
            extra, date_filter, bd_col, business_date_value, use_bd=use_bd
        )
        cond = f"{col} IS NOT NULL AND {col} <> TRIM({col})"
        if where_clause:
            where_clause = where_clause + f" AND ({cond})"
        else:
            where_clause = f"WHERE {cond}"

        sql = f"SELECT COUNT(*) AS TRIM_VIOL_CNT FROM {fqn} {where_clause}"
        sql_list.append(sql)
        try:
            rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
            total_dur += dur
            cnt = rows[0]["TRIM_VIOL_CNT"] if rows else 0
            total_viol += cnt
        except Exception as e:
            error = f"Error on column {col}: {str(e)[:3000]}"
            break

    combined_sql = ";\n".join(sql_list)
    passed = (error is None) and (total_viol == 0)
    metrics = (
        None if error else {"TOTAL_TRIM_VIOLATIONS": total_viol, "COLUMNS": trim_cols}
    )

    _insert_result_row(
        session,
        run_name,
        parent_db,
        schema,
        table,
        business_date_value,
        test_name,
        combined_sql,
        metrics,
        passed,
        error,
        total_dur if error is None else None,
        debug,
    )


def _test_cleaned_cols(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    """
    Check for presence of newline or tab characters in specified columns.
    """
    test_name = "CLEANED_COLS"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    clean_cols = tbl_cfg.get("clean_columns") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, clean_cols={clean_cols}"
        )

    if not clean_cols:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "clean_columns not specified",
            None,
            debug,
        )
        return

    total_bad = 0
    sql_list: List[str] = []
    error = None
    total_dur = 0

    for col in clean_cols:
        where_clause = _build_where_clause(
            extra, date_filter, bd_col, business_date_value, use_bd=use_bd
        )
        cond = (
            f"{col} IS NOT NULL AND "
            f"(REGEXP_LIKE({col}, '\\\\n') OR REGEXP_LIKE({col}, '\\\\t'))"
        )
        if where_clause:
            where_clause = where_clause + f" AND ({cond})"
        else:
            where_clause = f"WHERE {cond}"

        sql = f"SELECT COUNT(*) AS BAD_CNT FROM {fqn} {where_clause}"
        sql_list.append(sql)
        try:
            rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
            total_dur += dur
            cnt = rows[0]["BAD_CNT"] if rows else 0
            total_bad += cnt
        except Exception as e:
            error = f"Error on column {col}: {str(e)[:3000]}"
            break

    combined_sql = ";\n".join(sql_list)
    passed = (error is None) and (total_bad == 0)
    metrics = None if error else {"TOTAL_BAD": total_bad, "COLUMNS": clean_cols}

    _insert_result_row(
        session,
        run_name,
        parent_db,
        schema,
        table,
        business_date_value,
        test_name,
        combined_sql,
        metrics,
        passed,
        error,
        total_dur if error is None else None,
        debug,
    )


def _test_single_open_record(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "SINGLE_OPEN_RECORD"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    scd = tbl_cfg.get("scd") or {}

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, scd={scd}"
        )

    nat_keys = scd.get("natural_key_columns") or []
    end_col = scd.get("end_date_column")
    open_end = scd.get("open_end_value")

    if not nat_keys or not end_col:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "scd.natural_key_columns and scd.end_date_column required",
            None,
            debug,
        )
        return

    nat_list = ", ".join(nat_keys)
    where_clause = _build_where_clause(
        extra, date_filter, bd_col, business_date_value, use_bd=use_bd
    )

    if open_end is None:
        open_cond = f"{end_col} IS NULL"
    else:
        open_cond = f"({end_col} IS NULL OR {end_col} = '{open_end}')"

    if where_clause:
        where_clause = where_clause + f" AND {open_cond}"
    else:
        where_clause = f"WHERE {open_cond}"

    sql = f"""
        SELECT COUNT(*) AS BAD_KEY_CNT
        FROM (
          SELECT {nat_list}, COUNT(*) AS OPEN_REC_CNT
          FROM {fqn}
          {where_clause}
          GROUP BY {nat_list}
          HAVING COUNT(*) > 1
        ) T
    """

    try:
        rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
        bad_cnt = rows[0]["BAD_KEY_CNT"] if rows else 0
        passed = bad_cnt == 0
        metrics = {"BAD_KEY_CNT": bad_cnt}
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            metrics,
            passed,
            None,
            dur,
            debug,
        )
    except Exception as e:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            sql,
            None,
            False,
            str(e)[:4000],
            None,
            debug,
        )


def _test_fk_no_orphans(
    session,
    parent_db,
    tbl_cfg,
    run_name,
    business_date_value,
    date_filter,
    use_bd,
    debug,
):
    test_name = "FK_NO_ORPHANS"
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    child_fqn = _fqn(parent_db, schema, table)
    extra = tbl_cfg.get("extra_filter")
    bd_col = tbl_cfg.get("business_date_column")
    fk_list = tbl_cfg.get("fk_relations") or []

    if debug:
        print(
            f"[DEBUG][{test_name}] Starting for {parent_db}.{schema}.{table}, fk_relations={fk_list}"
        )

    if not fk_list:
        _insert_result_row(
            session,
            run_name,
            parent_db,
            schema,
            table,
            business_date_value,
            test_name,
            None,
            None,
            False,
            "fk_relations not specified",
            None,
            debug,
        )
        return

    for fk in fk_list:
        fk_name = fk.get("fk_name") or "FK"
        child_col = fk.get("child_column")
        parent_schema = fk.get("parent_schema")
        parent_table = fk.get("parent_table")
        parent_key = fk.get("parent_key_column")

        if not (child_col and parent_schema and parent_table and parent_key):
            _insert_result_row(
                session,
                run_name,
                parent_db,
                schema,
                table,
                business_date_value,
                test_name,
                None,
                None,
                False,
                f"Incomplete fk_relations config for {fk_name}",
                None,
                debug,
            )
            continue

        parent_fqn = _fqn(parent_db, parent_schema, parent_table)
        where_clause = _build_where_clause(
            extra, date_filter, bd_col, business_date_value, use_bd=use_bd
        )

        cond = f"P.{parent_key} IS NULL AND C.{child_col} IS NOT NULL"
        if where_clause:
            where_clause = where_clause + f" AND {cond}"
        else:
            where_clause = "WHERE " + cond

        sql = f"""
            SELECT COUNT(*) AS ORPHAN_CNT
            FROM (
              SELECT DISTINCT C.{child_col}
              FROM {child_fqn} C
              LEFT JOIN {parent_fqn} P
                ON C.{child_col} = P.{parent_key}
              {where_clause}
            ) O
        """

        try:
            rows, dur = _run_sql_with_timing(session, sql, debug, test_name)
            orphan_cnt = rows[0]["ORPHAN_CNT"] if rows else 0
            passed = orphan_cnt == 0
            metrics = {"FK_NAME": fk_name, "ORPHAN_CNT": orphan_cnt}
            _insert_result_row(
                session,
                run_name,
                parent_db,
                schema,
                table,
                business_date_value,
                test_name,
                sql,
                metrics,
                passed,
                None,
                dur,
                debug,
            )
        except Exception as e:
            _insert_result_row(
                session,
                run_name,
                parent_db,
                schema,
                table,
                business_date_value,
                test_name,
                sql,
                None,
                False,
                f"{fk_name} error: {str(e)[:4000]}",
                None,
                debug,
            )


# =====================================================================
# TEST REGISTRY
# =====================================================================

TEST_REGISTRY: Dict[str, Any] = {
    "BUSINESS_DATE_MATCH": _test_business_date_match,
    "NON_ZERO_COUNT_FOR_BUSINESS_DATE": _test_non_zero_count_for_bd,
    "DUPLICATE_PKS": _test_duplicate_pks,
    "PK_NOT_NULL": _test_pk_not_null,
    "DATE_COLS_NOT_NULL": _test_date_cols_not_null,
    "TIMESTAMP_COLS_NOT_NULL": _test_timestamp_cols_not_null,
    "TRIMMED_COLS": _test_trimmed_cols,
    "CLEANED_COLS": _test_cleaned_cols,
    "SINGLE_OPEN_RECORD": _test_single_open_record,
    "FK_NO_ORPHANS": _test_fk_no_orphans,
}

# =====================================================================
# Engine + wrapper
# =====================================================================


def run_shakedown(
    session: Session, meta: Dict[str, Any], business_date_value: Optional[str]
):
    """
    Generic engine: runs tests for ONE table based on meta + business_date_value.
    Applies BD rule: '1900-01-01' = no BD filter.
    """
    _ensure_results_table(session)

    run_name = meta.get("run_name", "shakedown_run")
    parent_db = meta.get("parent_db", PARENT_DB)
    tbl_cfg = deepcopy(meta["table"])
    schema = tbl_cfg["schema"]
    table = tbl_cfg["name"]
    date_filter = meta.get("date_filter")
    debug_mode = meta.get("debug_mode", "NO").upper() == "YES"

    requested = meta.get("tests_to_run") or list(TEST_REGISTRY.keys())
    active_tests = [t for t in requested if t in TEST_REGISTRY]

    use_bd = _should_apply_business_date(business_date_value)

    if debug_mode:
        print(
            f"[DEBUG] run_name={run_name}, db={parent_db}, table={schema}.{table}, "
            f"bd_value={business_date_value}, use_bd={use_bd}"
        )
        print(f"[DEBUG] active_tests={active_tests}")
        print(f"[DEBUG] date_filter={date_filter}")

    for test_name in active_tests:
        test_func = TEST_REGISTRY[test_name]
        if debug_mode:
            print(f"[DEBUG] >>> Running test: {test_name}")
        test_func(
            session,
            parent_db,
            tbl_cfg,
            run_name,
            business_date_value,
            date_filter,
            use_bd,
            debug_mode,
        )

    return session.sql(f"""
        SELECT
            RESULT_ID,
            RUN_TS,
            RUN_NAME,
            TABLE_DB,
            TABLE_SCHEMA,
            TABLE_NAME,
            BUSINESS_DATE,
            TEST_NAME,
            PASS_FLAG,
            ERROR,
            METRICS,
            DURATION_MS
        FROM QA_SHAKEDOWN_RESULTS
        WHERE RUN_NAME    = {_escape_sql_literal(run_name)}
          AND TABLE_DB    = {_escape_sql_literal(parent_db)}
          AND TABLE_SCHEMA = {_escape_sql_literal(schema)}
          AND TABLE_NAME   = {_escape_sql_literal(table)}
        ORDER BY RESULT_ID
    """)


def run_table_shakedown(
    session: Session, db_name: str, table_fqn: str, business_date_value: Optional[str]
):
    """
    Convenience wrapper:
      run_table_shakedown(session, "SESAME", "CORE.ORDERS", "2025-11-13")
      run_table_shakedown(session, "SESAME", "CORE.ORDERS", "1900-01-01")  # BD filter disabled
    """
    if "." not in table_fqn:
        raise ValueError("table_fqn must be like 'SCHEMA.TABLE' (no DB).")

    schema, table = table_fqn.split(".", 1)
    key = f"{schema}.{table}"
    if key not in TABLE_TEST_META:
        raise ValueError(f"No shakedown metadata defined for table {key}")

    parent_db = db_name or PARENT_DB

    base_meta = deepcopy(TABLE_TEST_META[key])
    run_name = (
        f"shakedown_{parent_db.lower()}_{schema.lower()}_{table.lower()}_"
        f"{(business_date_value or 'no_bd').replace('-','')}"
    )

    meta = {
        "run_name": run_name,
        "debug_mode": base_meta.get("debug_mode", "NO"),
        "parent_db": parent_db,
        "table": base_meta["table"],
        "tests_to_run": base_meta.get("tests_to_run", []),
        "date_filter": base_meta.get("date_filter"),
    }
    meta["table"]["schema"] = schema
    meta["table"]["name"] = table

    return run_shakedown(session, meta, business_date_value)


# =====================================================================
# Example usage in worksheet
# =====================================================================
# df = run_table_shakedown(session, "SESAME", "CORE.ORDERS", "2025-11-13")
# df.show()
# df = run_table_shakedown(session, "SESAME", "CORE.ORDERS", "1900-01-01")  # no BD filter
# df.show()


df = run_table_shakedown(session, "SESAME", "CORE.ORDERS", "2025-11-13")
df.show()
