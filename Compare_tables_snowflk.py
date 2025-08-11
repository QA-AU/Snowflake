#1  Control table (DDL)
CREATE OR REPLACE TABLE META_TABLE_MAP (
  RULE_ID            NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  S_SCHEMA           STRING         NOT NULL,
  S_TABLE            STRING         NOT NULL,
  S_IGNORE_COLUMNS   ARRAY,         -- store as JSON array: ["COL_A","COL_B"]
  T_SCHEMA           STRING         NOT NULL,
  T_TABLE            STRING         NOT NULL,
  T_IGNORE_COLUMNS   ARRAY,         -- same format
  IS_ACTIVE          BOOLEAN        DEFAULT TRUE,
  NOTE               STRING,
  CREATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

#2 Insert values 
INSERT INTO META_TABLE_MAP
  (S_SCHEMA, S_TABLE, S_IGNORE_COLUMNS, T_SCHEMA, T_TABLE, T_IGNORE_COLUMNS, NOTE)
VALUES
  ('RAW', 'CUSTOMERS', PARSE_JSON('["LOAD_TS","_INGEST_ID"]'),
   'DW',  'DIM_CUSTOMER', PARSE_JSON('["_ETL_TS","HASHDIFF"]'),
   'Ignore ETL/system columns');


#3 Prep compare proc

CREATE OR REPLACE PROCEDURE RUN_TABLE_COMPARE_PY()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session

# ======= Configure permanent output schema =======
OUT_SCHEMA = 'UTIL'   # change to your preferred schema (must be writable)
# =================================================

def q(name: str) -> str:
    return '"' + str(name).replace('"','""') + '"'

def sql_literal(val):
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def parse_cols_list(cols_csv_quoted: str):
    # '"A","B","C"' -> ['A','B','C'] uppercased
    if not cols_csv_quoted:
        return []
    parts = [c.strip() for c in cols_csv_quoted.split(',')]
    clean = [p[1:-1] if p.startswith('"') and p.endswith('"') else p for p in parts]
    return [c.upper() for c in clean]

def list_cols_excluding(session: Session, schema: str, table: str, row_id: int, side: str) -> str:
    """
    Returns comma-separated, quoted column list for schema.table excluding ignore columns.
    If ignore list is NULL/empty → excludes nothing (compare all). Keeps ordinal order.
    """
    ignore_expr = "COALESCE(m.S_IGNORE_COLUMNS, ARRAY_CONSTRUCT())" if side == 'S' \
                  else "COALESCE(m.T_IGNORE_COLUMNS, ARRAY_CONSTRUCT())"
    sql = f"""
        WITH m AS (SELECT * FROM META_TABLE_MAP WHERE ROW_ID = :rid)
        SELECT LISTAGG('"'||c.COLUMN_NAME||'"', ',') WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
        FROM INFORMATION_SCHEMA.COLUMNS c, m
        WHERE c.TABLE_SCHEMA = :schema
          AND c.TABLE_NAME   = :table
          AND NOT EXISTS (
            SELECT 1
            FROM LATERAL FLATTEN(INPUT => {ignore_expr}) f
            WHERE UPPER(c.COLUMN_NAME) = UPPER(f.VALUE::STRING)
          )
    """
    rows = session.sql(sql).bind({"rid": row_id, "schema": schema, "table": table}).collect()
    return rows[0][0] if rows and rows[0][0] is not None else ""

def run(session: Session) -> str:
    # Ensure output schema exists
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {q(OUT_SCHEMA)}").collect()

    # Create a RUN_ID for this whole execution (timestamp-based, unique per call)
    RUN_ID = session.sql("SELECT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3')").collect()[0][0]
    print(f"RUN_ID={RUN_ID}")

    # Get ALL mappings (ignore IS_ACTIVE)
    rules = session.sql("""
        SELECT ROW_ID, UPPER(S_SCHEMA), UPPER(S_TABLE), UPPER(T_SCHEMA), UPPER(T_TABLE)
        FROM META_TABLE_MAP
        ORDER BY ROW_ID
    """).collect()

    processed = 0

    for row_id, s_schema, s_table, t_schema, t_table in rules:
        print(f"Executing for ROW_ID={row_id}, Source={s_schema}.{s_table}, Target={t_schema}.{t_table}")

        # -----------------------------------------------------------
        # Step #1: Count records in source and target; update RESULT_TABLE
        # -----------------------------------------------------------
        s_count = session.sql(f"SELECT COUNT(*) FROM {q(s_schema)}.{q(s_table)}").collect()[0][0]
        t_count = session.sql(f"SELECT COUNT(*) FROM {q(t_schema)}.{q(t_table)}").collect()[0][0]
        session.sql("""
            UPDATE RESULT_TABLE
               SET S_COUNT = :sc, T_COUNT = :tc, RUN_DATE = CURRENT_TIMESTAMP()
             WHERE ROW_ID = :rid
        """).bind({"sc": s_count, "tc": t_count, "rid": row_id}).collect()

        # -----------------------------------------------------------
        # Step #2: Prep SQL (SELECT all columns except ignore lists; order assumed same)
        # -----------------------------------------------------------
        src_cols = list_cols_excluding(session, s_schema, s_table, row_id, 'S')
        tgt_cols = list_cols_excluding(session, t_schema, t_table, row_id, 'T')
        if not src_cols or not tgt_cols:
            session.sql("""
                UPDATE RESULT_TABLE
                   SET RESULT = 'ERROR', RUN_DATE = CURRENT_TIMESTAMP()
                 WHERE ROW_ID = :rid
            """).bind({"rid": row_id}).collect()
            processed += 1
            continue

        src_sql = f"SELECT {src_cols} FROM {q(s_schema)}.{q(s_table)}"
        tgt_sql = f"SELECT {tgt_cols} FROM {q(t_schema)}.{q(t_table)}"

        # Temp views (for EXCEPT)
        src_view = f"SRC_V_{row_id}"
        tgt_view = f"TGT_V_{row_id}"
        session.sql(f"CREATE OR REPLACE TEMP VIEW {q(src_view)} AS {src_sql}").collect()
        session.sql(f"CREATE OR REPLACE TEMP VIEW {q(tgt_view)} AS {tgt_sql}").collect()

        # -----------------------------------------------------------
        # Step #3: Symmetric EXCEPT → temp diff table DIFF_TEMP_<row_id>
        # -----------------------------------------------------------
        diff_temp = f"DIFF_TEMP_{row_id}"
        session.sql(f"""
            CREATE OR REPLACE TEMP TABLE {q(diff_temp)} AS
            (
              (SELECT {src_cols} FROM {q(src_view)} EXCEPT SELECT {tgt_cols} FROM {q(tgt_view)})
              UNION ALL
              (SELECT {tgt_cols} FROM {q(tgt_view)} EXCEPT SELECT {src_cols} FROM {q(src_view)})
            )
        """).collect()

        # -----------------------------------------------------------
        # Step #4: Count records in diff_temp; update ROWS_DIFFERENT
        # -----------------------------------------------------------
        diff_count = session.sql(f"SELECT COUNT(*) FROM {q(diff_temp)}").collect()[0][0]
        session.sql("""
            UPDATE RESULT_TABLE
               SET ROWS_DIFFERENT = :dc, RUN_DATE = CURRENT_TIMESTAMP()
             WHERE ROW_ID = :rid
        """).bind({"dc": diff_count, "rid": row_id}).collect()

        # -----------------------------------------------------------
        # Step #4.x: If differences exist, build PK-based sample; persist in OUT_SCHEMA
        # -----------------------------------------------------------
        sample_tbl_fqn = None
        trans_tbl_fqn  = None
        aligned_cols_list = parse_cols_list(src_cols)
        pk_cols = []  # default if not configured

        if int(diff_count) > 0:
            pk_row = session.sql("SELECT PRIMARY_KEY FROM META_TABLE_MAP WHERE ROW_ID = :rid") \
                            .bind({"rid": row_id}).collect()
            pk_arr = pk_row[0][0] if pk_row else None
            pk_cols = [str(x).upper() for x in (pk_arr or [])]

            if pk_cols:
                # anchor: one diff row
                diff_one = f"DIFF_ONE_{row_id}"
                session.sql(f"CREATE OR REPLACE TEMP TABLE {q(diff_one)} AS SELECT * FROM {q(diff_temp)} LIMIT 1").collect()

                # Are PK columns in projection?
                pk_in_projection = all(pk in aligned_cols_list for pk in pk_cols)

                pk_vals = None
                if pk_in_projection:
                    sel = ", ".join([q(c) for c in pk_cols])
                    r = session.sql(f"SELECT {sel} FROM {q(diff_one)} LIMIT 1").collect()
                    if r:
                        pk_vals = dict(zip(pk_cols, list(r[0])))
                else:
                    # derive from SRC then TGT (NULL-safe join)
                    join_on = " AND ".join([f"s.{q(c)} IS NOT DISTINCT FROM d.{q(c)}" for c in aligned_cols_list])
                    sel_src = ", ".join([f"s.{q(c)}" for c in pk_cols])
                    r = session.sql(
                        f"SELECT {sel_src} FROM {q(s_schema)}.{q(s_table)} s "
                        f"JOIN {q(diff_one)} d ON {join_on} LIMIT 1"
                    ).collect()
                    if r:
                        pk_vals = dict(zip(pk_cols, list(r[0])))
                    else:
                        sel_tgt = ", ".join([f"t.{q(c)}" for c in pk_cols])
                        r = session.sql(
                            f"SELECT {sel_tgt} FROM {q(t_schema)}.{q(t_table)} t "
                            f"JOIN {q(diff_one)} d ON {join_on} LIMIT 1"
                        ).collect()
                        if r:
                            pk_vals = dict(zip(pk_cols, list(r[0])))

                if pk_vals:
                    # WHERE clause
                    conds = []
                    for c in pk_cols:
                        v = pk_vals.get(c)
                        conds.append(f"{q(c)} IS NULL" if v is None else f"{q(c)} = {sql_literal(v)}")
                    where_clause = " WHERE " + " AND ".join(conds)

                    # PK alias list (PK_<col>)
                    pk_alias = [f"{q(c)} AS {q('PK_' + c)}" for c in pk_cols]

                    # ---- Persisted sample (two rows) with RUN_ID & ROW_ID ----
                    sample_tbl_name = f"SAMPLE_{row_id}_{RUN_ID}"
                    sample_tbl_fqn  = f"{q(OUT_SCHEMA)}.{q(sample_tbl_name)}"
                    session.sql(
                        f"""
                        CREATE OR REPLACE TABLE {sample_tbl_fqn} AS
                        (
                          SELECT '{RUN_ID}' AS RUN_ID, {row_id}::NUMBER AS ROW_ID, 'SOURCE' AS SIDE,
                                 {src_cols}{(',' if src_cols else '')} {', '.join(pk_alias)}
                          FROM {q(s_schema)}.{q(s_table)} {where_clause}
                          UNION ALL
                          SELECT '{RUN_ID}' AS RUN_ID, {row_id}::NUMBER AS ROW_ID, 'TARGET' AS SIDE,
                                 {tgt_cols}{(',' if tgt_cols else '')} {', '.join(pk_alias)}
                          FROM {q(t_schema)}.{q(t_table)} {where_clause}
                        )
                        """
                    ).collect()

                    # Record table name(s) in RESULT_TABLE
                    session.sql("""
                        UPDATE RESULT_TABLE
                           SET SAMPLE_OUTPUT_TABLE = :tname, RUN_DATE = CURRENT_TIMESTAMP()
                         WHERE ROW_ID = :rid
                    """).bind({"tname": f"{OUT_SCHEMA}.SAMPLE_{row_id}_{RUN_ID}", "rid": row_id}).collect()
                else:
                    session.sql("""
                        UPDATE RESULT_TABLE
                           SET SAMPLE_OUTPUT_TABLE = 'PK_NOT_FOUND_FOR_SAMPLE', RUN_DATE = CURRENT_TIMESTAMP()
                         WHERE ROW_ID = :rid
                    """).bind({"rid": row_id}).collect()
            else:
                session.sql("""
                    UPDATE RESULT_TABLE
                       SET SAMPLE_OUTPUT_TABLE = 'NO_PRIMARY_KEY_CONFIGURED', RUN_DATE = CURRENT_TIMESTAMP()
                     WHERE ROW_ID = :rid
                """).bind({"rid": row_id}).collect()

        # -----------------------------------------------------------
        # Step #4.y: Transpose sample → OUT_SCHEMA.SAMPLE_T_<row_id>_<run_id> with MATCH/MISMATCH + PK columns
        # -----------------------------------------------------------
        if int(diff_count) > 0 and sample_tbl_fqn:
            obj_pairs = ", ".join([f"'{c}', TO_VARCHAR({q(c)})" for c in aligned_cols_list])

            # Build PK column projection as constants from SOURCE row in sample table
            pk_alias_cols = []
            for c in pk_cols:
                alias = 'PK_' + c
                pk_alias_cols.append(f"(SELECT {q(alias)} FROM {sample_tbl_fqn} WHERE SIDE='SOURCE' LIMIT 1) AS {q(alias)}")

            trans_tbl_name = f"SAMPLE_T_{row_id}_{RUN_ID}"
            trans_tbl_fqn  = f"{q(OUT_SCHEMA)}.{q(trans_tbl_name)}"

            session.sql(f"""
                CREATE OR REPLACE TABLE {trans_tbl_fqn} AS
                WITH s AS (
                  SELECT OBJECT_CONSTRUCT({obj_pairs}) AS OBJ
                  FROM {sample_tbl_fqn} WHERE SIDE='SOURCE' LIMIT 1
                ),
                t AS (
                  SELECT OBJECT_CONSTRUCT({obj_pairs}) AS OBJ
                  FROM {sample_tbl_fqn} WHERE SIDE='TARGET' LIMIT 1
                ),
                s2 AS (
                  SELECT f.KEY::STRING AS COLUMN_NAME, f.VALUE::STRING AS SRC_VALUE
                  FROM s, LATERAL FLATTEN(INPUT=>s.OBJ) f
                ),
                t2 AS (
                  SELECT f.KEY::STRING AS COLUMN_NAME, f.VALUE::STRING AS TGT_VALUE
                  FROM t, LATERAL FLATTEN(INPUT=>t.OBJ) f
                )
                SELECT
                  '{RUN_ID}' AS RUN_ID,
                  {row_id}::NUMBER AS ROW_ID,
                  COALESCE(s2.COLUMN_NAME, t2.COLUMN_NAME) AS COLUMN_NAME,
                  s2.SRC_VALUE,
                  t2.TGT_VALUE,
                  CASE
                    WHEN s2.SRC_VALUE IS NOT DISTINCT FROM t2.TGT_VALUE THEN 'MATCH'
                    ELSE 'MISMATCH'
                  END AS MATCH_STATUS
                  {(',' if pk_alias_cols else '')} {', '.join(pk_alias_cols)}
                FROM s2
                FULL OUTER JOIN t2
                  ON s2.COLUMN_NAME = t2.COLUMN_NAME
                ORDER BY COLUMN_NAME
            """).collect()

            print(f"Created transpose table: {OUT_SCHEMA}.SAMPLE_T_{row_id}_{RUN_ID}")

            # Append both sample table names (row + transpose) to RESULT_TABLE
            session.sql("""
                UPDATE RESULT_TABLE
                   SET SAMPLE_OUTPUT_TABLE = IFF(SAMPLE_OUTPUT_TABLE IS NULL,
                                                 :names,
                                                 SAMPLE_OUTPUT_TABLE || ',' || :names),
                       RUN_DATE = CURRENT_TIMESTAMP()
                 WHERE ROW_ID = :rid
            """).bind({"names": f"{OUT_SCHEMA}.SAMPLE_{row_id}_{RUN_ID},{OUT_SCHEMA}.SAMPLE_T_{row_id}_{RUN_ID}",
                       "rid": row_id}).collect()

        # -----------------------------------------------------------
        # Step #5: Pass/Fail logic
        # -----------------------------------------------------------
        if int(s_count) == int(t_count) and int(s_count) > 0 and int(diff_count) == 0:
            result_val = 'PASS'
        else:
            result_val = 'FAIL - Investigate'

        session.sql("""
            UPDATE RESULT_TABLE
               SET RESULT = :res, RUN_DATE = CURRENT_TIMESTAMP()
             WHERE ROW_ID = :rid
        """).bind({"res": result_val, "rid": row_id}).collect()

        processed += 1

    return f"Processed rows: {processed}; RUN_ID={RUN_ID}"
$$;



#4 : How to use

-- Run the compare
CALL RUN_TABLE_COMPARE_PY();

-- Find the latest sample tables created for a given ROW_ID
SELECT ROW_ID, SAMPLE_OUTPUT_TABLE
FROM RESULT_TABLE
WHERE SAMPLE_OUTPUT_TABLE IS NOT NULL
ORDER BY RUN_DATE DESC;

-- Inspect a sample quickly (replace 7 and the RUN_ID with your actual values)
SELECT * FROM UTIL.SAMPLE_7_20250811091523456;
SELECT * FROM UTIL.SAMPLE_T_7_20250811091523456;


What it does
Compares each mapping in META_TABLE_MAP (source→target), updates RESULT_TABLE, and—on a mismatch—writes two sample tables to a permanent schema (default UTIL) with a unique RUN_ID.
How to run (30 seconds)
Make sure META_TABLE_MAP is populated (ignore lists & PRIMARY_KEY are JSON arrays; NULL/[] = compare all cols).
Ensure RESULT_TABLE exists (copy of META + result columns).
Call the proc:
CALL RUN_TABLE_COMPARE_PY();
You’ll see a printed RUN_ID and “Executing for ROW_ID=…” lines in the Messages panel.
Where to find results
Summary (all rules)
SELECT ROW_ID, S_SCHEMA, S_TABLE, T_SCHEMA, T_TABLE,
       S_COUNT, T_COUNT, ROWS_DIFFERENT, RESULT, SAMPLE_OUTPUT_TABLE, RUN_DATE
FROM RESULT_TABLE
ORDER BY RUN_DATE DESC, ROW_ID;
PASS when S_COUNT == T_COUNT and S_COUNT > 0 and ROWS_DIFFERENT = 0.
Otherwise FAIL - Investigate.
Deep-dive (failed rules only)
For each failed rule, RESULT_TABLE.SAMPLE_OUTPUT_TABLE holds two comma-separated table names:
UTIL.SAMPLE_<row_id>_<run_id> – 2 rows (SOURCE vs TARGET) for one differing PK
UTIL.SAMPLE_T_<row_id>_<run_id> – transposed, one row per column with MATCH/MISMATCH
Example:
-- See latest failures
SELECT ROW_ID, SAMPLE_OUTPUT_TABLE
FROM RESULT_TABLE
WHERE RESULT <> 'PASS'
ORDER BY RUN_DATE DESC;

-- Inspect the row-level sample
SELECT * FROM UTIL.SAMPLE_7_20250811091523456;

-- Inspect the transposed per-column view
SELECT * FROM UTIL.SAMPLE_T_7_20250811091523456;
Notes
Output schema is set at the top of the proc (OUT_SCHEMA, default UTIL).
Both sample tables include RUN_ID, ROW_ID, and PK_<col> columns.
If ignore columns aren’t provided, the tool compares all columns in physical order.