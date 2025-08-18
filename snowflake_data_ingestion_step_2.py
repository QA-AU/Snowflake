snowflake_data_ingestion_step_2.py

# === STEP 2: Source slice materialisation & stats ===
# Sub-parts:
#   A) Resolve mapping (by target OR by source) from meta table
#   B) Load metadata rows for the resolved mapping
#   C) Validate source table & required columns
#   D) Build filters (s_filter + runtime window) with guardrail
#   E) Materialise TEMP view SRC_SLICE
#   F) Compute and print stats (rows, PK quality, lifecycle)

from snowflake.snowpark import Session
import pandas as pd

# ---------- RUNTIME SELECTION (choose ONE path) ----------
# Option A: target-driven
RUN_TLAYER    = "tgt_dw"         # target schema where META_COLUMNS lives too
RUN_TGT_TABLE = "dim_customer"   # target table to process

# Option B: source-driven
RUN_SLAYER    = None             # e.g., "src_stg"
RUN_SRC_TABLE = None             # e.g., "customer_stg"

# Optional runtime window predicate (ANDed with s_filter from meta)
WINDOW_PREDICATE = None          # e.g., "load_date = '2025-08-18'" or None

# Sanity: Snowpark session must exist
assert 'session' in globals(), "ERROR: Snowflake session not found. Ensure the Notebook created a Snowpark session."


# ----------------------------------------------------------------------
# Part A) Resolve mapping
# ----------------------------------------------------------------------
print("Part A — Resolving mapping from META_COLUMNS ...")

if RUN_TLAYER and RUN_TGT_TABLE:
    ident = f"target {RUN_TLAYER}.{RUN_TGT_TABLE}"
    map_sql = f"""
        SELECT DISTINCT slayer, srctablename, tlayer, tgtable
        FROM {RUN_TLAYER}.META_COLUMNS
        WHERE UPPER(tlayer)=UPPER('{RUN_TLAYER}')
          AND UPPER(tgtable)=UPPER('{RUN_TGT_TABLE}')
    """
elif RUN_SLAYER and RUN_SRC_TABLE:
    ident = f"source {RUN_SLAYER}.{RUN_SRC_TABLE}"
    # If your meta table is always in RUN_TLAYER, reference it there; otherwise adjust schema accordingly.
    map_sql = f"""
        SELECT DISTINCT slayer, srctablename, tlayer, tgtable
        FROM {RUN_TLAYER}.META_COLUMNS
        WHERE UPPER(slayer)=UPPER('{RUN_SLAYER}')
          AND UPPER(srctablename)=UPPER('{RUN_SRC_TABLE}')
    """
else:
    raise SystemExit("ERROR: Provide either (RUN_TLAYER & RUN_TGT_TABLE) OR (RUN_SLAYER & RUN_SRC_TABLE).")

mapping = session.sql(map_sql).to_pandas()
if mapping.empty:
    raise SystemExit(f"ERROR: No mapping found for {ident} in {RUN_TLAYER}.META_COLUMNS")
if len(mapping) > 1:
    raise SystemExit(f"ERROR: Multiple mappings found for {ident}; refine selection.")

SLAYER   = mapping.iloc[0]["SLAYER"]
SRCTABLE = mapping.iloc[0]["SRCTABLENAME"]
TLAYER   = mapping.iloc[0]["TLAYER"]
TGTTABLE = mapping.iloc[0]["TGTTABLE"]

print(f"✓ Using mapping: {SLAYER}.{SRCTABLE} → {TLAYER}.{TGTTABLE}")


# ----------------------------------------------------------------------
# Part B) Load metadata rows for this mapping
# ----------------------------------------------------------------------
print("Part B — Loading metadata rows for the resolved mapping ...")

meta_sql = f"""
SELECT *
FROM {TLAYER}.META_COLUMNS
WHERE UPPER(slayer)=UPPER('{SLAYER}')
  AND UPPER(srctablename)=UPPER('{SRCTABLE}')
  AND UPPER(tlayer)=UPPER('{TLAYER}')
  AND UPPER(tgtable)=UPPER('{TGTTABLE}')
"""
meta = session.sql(meta_sql).to_pandas()
if meta.empty:
    raise SystemExit(f"ERROR: No META rows for mapping {SLAYER}.{SRCTABLE} → {TLAYER}.{TGTTABLE}")

# Derive roles
pk_cols        = meta.loc[meta["PRIMARYKEY"].str.upper()=="Y", "SRCCOLUMNNAME"].str.upper().tolist()
ignore_cols    = meta.loc[meta["IGNORE_ROW"].str.upper()=="Y", "SRCCOLUMNNAME"].str.upper().tolist()
use_scd2_cols  = meta.loc[meta["USE_SCD2"].str.upper()=="Y", "SRCCOLUMNNAME"].str.upper().tolist()
is_filter_cols = meta.loc[meta["IS_FILTER"].str.upper()=="Y", "SRCCOLUMNNAME"].str.upper().tolist()
del_cols       = meta.loc[meta["IS_DELETED"].str.upper()=="Y", "SRCCOLUMNNAME"].str.upper().tolist()

# Lifecycle names
has_from = (meta["SRCCOLUMNNAME"].str.lower() == "from_date").any()
has_end  = (meta["SRCCOLUMNNAME"].str.lower() == "end_date").any()
from_col = "FROM_DATE" if has_from else None
end_col  = "END_DATE"  if has_end  else None
del_col  = del_cols[0] if len(del_cols) == 1 else None

# Free-text source filter (first non-null if present)
s_filters = meta["S_FILTER"].dropna().astype(str).str.strip()
s_filter_expr = s_filters.iloc[0] if not s_filters.empty and s_filters.iloc[0] else None

# Select list = all non-ignored source columns (upper-cased)
select_cols = meta.loc[meta["IGNORE_ROW"].str.upper()!="Y", "SRCCOLUMNNAME"].str.upper().tolist()
select_cols = list(dict.fromkeys(select_cols))
if not select_cols:
    raise SystemExit("ERROR: No selectable columns (all rows are ignored) in META.")


# ----------------------------------------------------------------------
# Part C) Validate source table & required columns
# ----------------------------------------------------------------------
print("Part C — Validating source table existence and required columns ...")

tbl_chk = session.sql(f"""
SELECT 1
FROM INFORMATION_SCHEMA.TABLES
WHERE UPPER(TABLE_SCHEMA)=UPPER('{SLAYER}')
  AND UPPER(TABLE_NAME)=UPPER('{SRCTABLE}')
""").collect()
if not tbl_chk:
    raise SystemExit(f"ERROR: Source table {SLAYER}.{SRCTABLE} does not exist.")

cols_df = session.sql(f"""
SELECT UPPER(COLUMN_NAME) AS COL
FROM INFORMATION_SCHEMA.COLUMNS
WHERE UPPER(TABLE_SCHEMA)=UPPER('{SLAYER}')
  AND UPPER(TABLE_NAME)=UPPER('{SRCTABLE}')
""").to_pandas()
src_cols = set(cols_df["COL"].tolist())

required = set(pk_cols + select_cols)
missing  = sorted(required - src_cols)
if missing:
    raise SystemExit(f"ERROR: Missing source columns in {SLAYER}.{SRCTABLE}: {missing}")

# Lifecycle feasibility
can_lifecycle = False
lifecycle_note = "N/A"
if (end_col and end_col in src_cols) or (del_col and del_col in src_cols):
    can_lifecycle = True
    lifecycle_note = f"end_date={'Y' if (end_col and end_col in src_cols) else 'N'}, is_deleted={'Y' if (del_col and del_col in src_cols) else 'N'}"


# ----------------------------------------------------------------------
# Part D) Build filters (s_filter + runtime window) & guardrail
# ----------------------------------------------------------------------
print("Part D — Building source WHERE filters ...")

predicates = []
if s_filter_expr:
    predicates.append(f"({s_filter_expr})")
if WINDOW_PREDICATE:
    predicates.append(f"({WINDOW_PREDICATE})")

if not predicates:
    print("WARNING: No s_filter or WINDOW_PREDICATE provided — running FULL SOURCE SCAN.")

where_sql = f" WHERE {' AND '.join(predicates)}" if predicates else ""
select_list = ", ".join([f'"{c}"' for c in select_cols])  # quote identifiers
src_sql = f'SELECT {select_list} FROM "{SLAYER}"."{SRCTABLE}"{where_sql}'


# ----------------------------------------------------------------------
# Part E) Materialise TEMP view SRC_SLICE
# ----------------------------------------------------------------------
print("Part E — Materialising TEMP view SRC_SLICE ...")

session.sql("DROP VIEW IF EXISTS SRC_SLICE").collect()
session.sql(f"CREATE TEMP VIEW SRC_SLICE AS {src_sql}").collect()


# ----------------------------------------------------------------------
# Part F) Compute stats & print summary
# ----------------------------------------------------------------------
print("Part F — Computing source stats & printing summary ...")

# Total rows
rows_total = session.sql("SELECT COUNT(*) AS C FROM SRC_SLICE").to_pandas()["C"][0]

# PK quality
if pk_cols:
    null_pred = " OR ".join([f"{c} IS NULL" for c in pk_cols])
    null_pk   = session.sql(f"SELECT COUNT(*) AS C FROM SRC_SLICE WHERE {null_pred}").to_pandas()["C"][0]
    pk_list   = ", ".join(pk_cols)
    dup_pk    = session.sql(f"""
        SELECT COUNT(*) AS C
        FROM (
            SELECT {pk_list}, COUNT(*) AS CNT
            FROM SRC_SLICE
            GROUP BY {pk_list}
            HAVING COUNT(*) > 1
        )
    """).to_pandas()["C"][0]
    distinct_pk = session.sql(f"SELECT COUNT(*) AS C FROM (SELECT DISTINCT {pk_list} FROM SRC_SLICE)").to_pandas()["C"][0]
    if dup_pk > 0:
        raise SystemExit(f"ERROR: Duplicate PKs in source slice for {SLAYER}.{SRCTABLE}. Duplicate groups={dup_pk}")
else:
    null_pk = dup_pk = distinct_pk = 0

# Lifecycle breakdown (if feasible)
open_cnt = closed_cnt = None
if can_lifecycle:
    if end_col and del_col:
        open_cnt   = session.sql(f"""
            SELECT COUNT(*) AS C
            FROM SRC_SLICE
            WHERE {end_col} = DATE '9999-12-31' AND UPPER({del_col})='N'
        """).to_pandas()["C"][0]
        closed_cnt = rows_total - open_cnt
    elif end_col:
        open_cnt   = session.sql(f"SELECT COUNT(*) AS C FROM SRC_SLICE WHERE {end_col} = DATE '9999-12-31'").to_pandas()["C"][0]
        closed_cnt = rows_total - open_cnt
    elif del_col:
        open_cnt   = session.sql(f"SELECT COUNT(*) AS C FROM SRC_SLICE WHERE UPPER({del_col})='N'").to_pandas()["C"][0]
        closed_cnt = rows_total - open_cnt

# Summary print
print("\nSTEP 2 SUMMARY")
print("--------------------------------------------------")
driver = "TARGET-DRIVEN" if (RUN_TLAYER and RUN_TGT_TABLE) else "SOURCE-DRIVEN"
print(f"Driver       : {driver}")
print(f"Source       : {SLAYER}.{SRCTABLE}")
print(f"Target (ctx) : {TLAYER}.{TGTTABLE}")
print(f"Selected cols: {select_cols}")
print(f"PK cols      : {pk_cols if pk_cols else 'NONE'}")
print(f"Rows (total) : {rows_total}")
if pk_cols:
    print(f"PK distinct  : {distinct_pk} | PK null rows : {null_pk} | PK duplicate groups : {dup_pk}")
print(f"Lifecycle    : {lifecycle_note}")
if can_lifecycle:
    print(f"Open/Closed  : OPEN={open_cnt} | CLOSED={closed_cnt}")
if predicates:
    print(f"Filters used : {' AND '.join(predicates)}")
else:
    print("Filters used : NONE")
print("--------------------------------------------------")
print("Successfully completed Step 2.")
