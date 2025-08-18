snowflake_data_ingestion_step_5.py

# === STEP 5: VALIDATION APPLY → build SIMULATED OUTPUT (no target DML) ===
# Sub-parts:
#   A) Inputs & meta (mode, columns, PKs, lifecycle cols)
#   B) HISTORY mode  -> SIM = (actual target) UNION ALL (mapped SRC_SLICE)
#   C) SCD2 mode     -> SIM = closed(old current rows) + new versions + untouched rows
#   D) Materialise SIM table in a validation schema
#   E) Print summary (planned state counts)

from snowflake.snowpark import Session
import pandas as pd

# -------- Config --------
VALIDATION_SCHEMA = "tgt_dw_val"                 # where SIM tables will live
SIM_TABLE         = None                          # will default to f"{TGTTABLE}_SIM"
# Set to "Y" if you only want a TEMP VIEW (SIM_VIEW) and skip writing a table:
SIM_AS_VIEW_ONLY = "N"

# -------- Pre-reqs from Steps 2-4 --------
assert 'session' in globals(), "ERROR: Snowflake session not found."
for v in ("TLAYER","TGTTABLE","SLAYER","SRCTABLE"):
    assert v in globals(), f"ERROR: var {v} not set. Run Steps 2–3 first."
for v in ("SRC_SLICE","TGT_SLICE","CLASSIFIED"):
    cnt = session.sql(f"""
        SELECT COUNT(*) C FROM INFORMATION_SCHEMA.VIEWS
        WHERE UPPER(TABLE_SCHEMA)=CURRENT_SCHEMA() AND UPPER(TABLE_NAME)=UPPER('{v}')
    """).to_pandas()["C"][0]
    if cnt == 0:
        raise SystemExit(f"ERROR: TEMP view {v} not found. Ensure Steps 2–4 ran.")

print("Part A — Loading meta & resolving mode/columns ...")

meta = session.sql(f"""
  SELECT
    UPPER(srccolumnname) AS SRC_COL,
    UPPER(tgtcolumnname) AS TGT_COL,
    UPPER(primarykey)    AS IS_PK,
    UPPER(ignore_row)    AS IGNORE_ROW,
    UPPER(use_scd2)      AS USE_SCD2,
    UPPER(history)       AS HISTORY
  FROM {TLAYER}.META_COLUMNS
  WHERE UPPER(tlayer)=UPPER('{TLAYER}')
    AND UPPER(tgtable)=UPPER('{TGTTABLE}')
""").to_pandas()
if meta.empty:
    raise SystemExit(f"ERROR: No META rows for {TLAYER}.{TGTTABLE}")

m_hist = meta["HISTORY"].dropna().unique().tolist()
if len(m_hist) != 1:
    raise SystemExit("ERROR: Mixed HISTORY flags in meta for this table.")
MODE = "HISTORY" if m_hist[0] == "Y" else "SCD2"
print(f"  Mode resolved: {MODE}")

# Build mappings/sets
pk_pairs = meta.loc[meta["IS_PK"]=="Y", ["SRC_COL","TGT_COL"]].drop_duplicates().values.tolist()
if not pk_pairs:
    raise SystemExit("ERROR: No primary keys defined in meta.")
pk_tgts = [p[1] for p in pk_pairs]

tgt_cols_all = meta.loc[meta["IGNORE_ROW"]!="Y","TGT_COL"].str.upper().drop_duplicates().tolist()
if not tgt_cols_all:
    raise SystemExit("ERROR: No writable target columns (all ignored).")
tgt_cols_csv = ", ".join([f'"{c}"' for c in tgt_cols_all])

# target physical columns (to know if IS_CURRENT/END_DATE/FROM_DATE exist)
tgt_physical_cols = set(session.sql(f"""
  SELECT UPPER(COLUMN_NAME) AS COL
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE UPPER(TABLE_SCHEMA)=UPPER('{TLAYER}')
    AND UPPER(TABLE_NAME)=UPPER('{TGTTABLE}')
""").to_pandas()["COL"].tolist())
has_is_current = "IS_CURRENT" in tgt_physical_cols
has_end_date   = "END_DATE"   in tgt_physical_cols
has_from_date  = "FROM_DATE"  in tgt_physical_cols

# Resolve "open" predicate (for current rows)
open_pred = None
if MODE == "SCD2":
    if has_is_current:
        open_pred = "UPPER(IS_CURRENT)='Y'"
    elif has_end_date:
        open_pred = "END_DATE = DATE '9999-12-31'"
    else:
        raise SystemExit("ERROR: SCD2 mode but target lacks IS_CURRENT/END_DATE to identify current rows.")
print(f"  Open predicate: {open_pred if open_pred else 'N/A (History)'}")

# Map SRC -> TGT select list for inserts
mapped_select = []
for _, row in meta.iterrows():
    if row["IGNORE_ROW"] == "Y":
        continue
    s, t = row["SRC_COL"], row["TGT_COL"]
    if MODE == "SCD2" and t == "END_DATE" and has_end_date:
        mapped_select.append("DATE '9999-12-31' AS END_DATE")
    elif MODE == "SCD2" and t == "IS_CURRENT" and has_is_current:
        mapped_select.append("'Y' AS IS_CURRENT")
    else:
        mapped_select.append(f"s.{s} AS {t}")
mapped_select_sql = ", ".join(mapped_select)

# Key list as CSV for joins
pk_tgts_csv = ", ".join([f"{k}" for k in pk_tgts])
pk_join_src_to_key = " AND ".join([f"s.{src} = k.{tgt}" for src,tgt in pk_pairs])

# Default SIM table name
if not SIM_TABLE:
    SIM_TABLE = f"{TGTTABLE}_SIM"

# Ensure validation schema
session.sql(f"CREATE SCHEMA IF NOT EXISTS {VALIDATION_SCHEMA}").collect()

if MODE == "HISTORY":
    print("Part B — HISTORY mode: building SIM as (actual target) UNION ALL (source batch mapped) ...")

    # Build HISTORY SIM as a view first
    hist_view_sql = f"""
    CREATE OR REPLACE TEMP VIEW SIM_VIEW AS
    SELECT * FROM "{TLAYER}"."{TGTTABLE}"
    UNION ALL
    SELECT {mapped_select_sql}
    FROM SRC_SLICE s
    """
    session.sql(hist_view_sql).collect()

else:
    print("Part C — SCD2 mode: building SIM as closed(old current) + new versions + untouched rows ...")

    # 1) Key sets from CLASSIFIED
    session.sql("CREATE OR REPLACE TEMP VIEW KEY_NEW      AS SELECT " + pk_tgts_csv + " FROM CLASSIFIED WHERE CLASSIFICATION='NEW'").collect()
    session.sql("CREATE OR REPLACE TEMP VIEW KEY_CHANGED  AS SELECT " + pk_tgts_csv + " FROM CLASSIFIED WHERE CLASSIFICATION='CHANGED'").collect()
    session.sql("CREATE OR REPLACE TEMP VIEW KEY_DELETED  AS SELECT " + pk_tgts_csv + " FROM CLASSIFIED WHERE CLASSIFICATION='DELETED'").collect()

    # 2) Current rows impacted
    impacted_pred = " OR ".join([
        "EXISTS (SELECT 1 FROM KEY_CHANGED kc WHERE " + " AND ".join([f"kc.{k}=t.{k}" for k in pk_tgts]) + ")",
        "EXISTS (SELECT 1 FROM KEY_DELETED kd WHERE " + " AND ".join([f"kd.{k}=t.{k}" for k in pk_tgts]) + ")"
    ])
    # 3) UNTOUCHED set = all rows except current impacted (keeps all prior history)
    untouched_sql = f"""
    CREATE OR REPLACE TEMP VIEW V_UNTOUCHED AS
    SELECT *
    FROM "{TLAYER}"."{TGTTABLE}" t
    WHERE NOT (
        ({open_pred}) AND ({impacted_pred})
    )
    """
    session.sql(untouched_sql).collect()

    # 4) CLOSED versions for CHANGED/DELETED (modify current rows virtually)
    close_changed_set = "DATEADD(DAY,-1, s.FROM_DATE)" if has_from_date else "CURRENT_DATE"
    close_deleted_set = "s.END_DATE" if has_end_date else "CURRENT_DATE"

    close_changed_sql = f"""
    CREATE OR REPLACE TEMP VIEW V_CLOSE_CHANGED AS
    SELECT
      t.*
      {", 'N' AS IS_CURRENT" if has_is_current else ""}
      {", " + close_changed_set + " AS END_DATE" if has_end_date else ""}
    FROM "{TLAYER}"."{TGTTABLE}" t
    JOIN TGT_SLICE tcur ON {" AND ".join([f"tcur.{k}=t.{k}" for k in pk_tgts])}
    JOIN KEY_CHANGED k  ON {" AND ".join([f"k.{k}=t.{k}" for k in pk_tgts])}
    JOIN SRC_SLICE s    ON {pk_join_src_to_key}
    WHERE ({open_pred})
    """
    if not has_is_current and not has_end_date:
        # If neither lifecycle column exists physically, we cannot model closure; fall back to carrying row as-is (rare)
        close_changed_sql = "CREATE OR REPLACE TEMP VIEW V_CLOSE_CHANGED AS SELECT * FROM TGT_SLICE WHERE 1=0"
    session.sql(close_changed_sql).collect()

    close_deleted_sql = f"""
    CREATE OR REPLACE TEMP VIEW V_CLOSE_DELETED AS
    SELECT
      t.*
      {", 'N' AS IS_CURRENT" if has_is_current else ""}
      {", " + close_deleted_set + " AS END_DATE" if has_end_date else ""}
    FROM "{TLAYER}"."{TGTTABLE}" t
    JOIN TGT_SLICE tcur ON {" AND ".join([f"tcur.{k}=t.{k}" for k in pk_tgts])}
    JOIN KEY_DELETED k  ON {" AND ".join([f"k.{k}=t.{k}" for k in pk_tgts])}
    LEFT JOIN SRC_SLICE s ON {pk_join_src_to_key}  -- may be absent in source
    WHERE ({open_pred})
    """
    if not has_is_current and not has_end_date:
        close_deleted_sql = "CREATE OR REPLACE TEMP VIEW V_CLOSE_DELETED AS SELECT * FROM TGT_SLICE WHERE 1=0"
    session.sql(close_deleted_sql).collect()

    # 5) NEW versions (for NEW + CHANGED) from source mapped
    new_versions_sql = f"""
    CREATE OR REPLACE TEMP VIEW V_NEW_VERSIONS AS
    SELECT {mapped_select_sql}
    FROM SRC_SLICE s
    WHERE EXISTS (SELECT 1 FROM KEY_NEW     k WHERE {pk_join_src_to_key})
       OR EXISTS (SELECT 1 FROM KEY_CHANGED k WHERE {pk_join_src_to_key})
    """
    session.sql(new_versions_sql).collect()

    # 6) UNCHANGED current rows (carry as-is)
    unchanged_sql = f"""
    CREATE OR REPLACE TEMP VIEW V_UNCHANGED AS
    SELECT t.*
    FROM TGT_SLICE t
    WHERE NOT EXISTS (SELECT 1 FROM KEY_CHANGED k WHERE {" AND ".join([f"k.{k}=t.{k}" for k in pk_tgts])})
      AND NOT EXISTS (SELECT 1 FROM KEY_DELETED k WHERE {" AND ".join([f"k.{k}=t.{k}" for k in pk_tgts])})
    """
    session.sql(unchanged_sql).collect()

    # 7) Assemble SIM as UNION ALL of components
    scd2_view_sql = f"""
    CREATE OR REPLACE TEMP VIEW SIM_VIEW AS
    SELECT * FROM V_UNTOUCHED
    UNION ALL
    SELECT * FROM V_CLOSE_CHANGED
    UNION ALL
    SELECT * FROM V_CLOSE_DELETED
    UNION ALL
    SELECT * FROM V_UNCHANGED
    UNION ALL
    SELECT * FROM V_NEW_VERSIONS
    """
    session.sql(scd2_view_sql).collect()

# -------- Materialise SIM --------
print("Part D — Materialising SIM output ...")

if SIM_AS_VIEW_ONLY.upper() == "Y":
    print("  SIM output kept as TEMP VIEW: SIM_VIEW")
else:
    session.sql(f'CREATE OR REPLACE TABLE {VALIDATION_SCHEMA}."{SIM_TABLE}" AS SELECT * FROM SIM_VIEW').collect()
    print(f'  SIM table created: {VALIDATION_SCHEMA}."{SIM_TABLE}"')

# -------- Summary --------
print("Part E — Summary of planned post-state ...")
post_cnt = session.sql("SELECT COUNT(*) C FROM SIM_VIEW").to_pandas()["C"][0]
print(f"  SIM rows (post-state): {post_cnt}")
print("Successfully completed Step 5 (validation-only simulation).")
