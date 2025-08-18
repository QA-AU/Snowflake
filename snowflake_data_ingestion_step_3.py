snowflake_data_ingestion_step_3.py

# === STEP 3: Target current slice & stats ===
# Sub-parts:
#   A) Resolve mapping (by target OR by source) from meta table
#   B) Load metadata rows for the resolved mapping
#   C) Validate target table & required columns
#   D) Build filters (t_filter + optional runtime window) with guardrail
#   E) Materialise TEMP view TGT_SLICE (current/open rows for SCD2; full table for History)
#   F) Compute and print stats (rows, PK quality, open/closed)

from snowflake.snowpark import Session
import pandas as pd

# ---------- RUNTIME SELECTION (choose ONE path; same as Step 2) ----------
# Option A: target-driven
RUN_TLAYER    = "tgt_dw"         # target schema (also where META_COLUMNS lives)
RUN_TGT_TABLE = "dim_customer"   # target table to process

# Option B: source-driven
RUN_SLAYER    = None             # e.g., "src_stg"
RUN_SRC_TABLE = None             # e.g., "customer_stg"

# Optional runtime window (often NOT used on target, but supported if you want symmetry)
WINDOW_PREDICATE = None          # e.g., "load_date = '2025-08-18'"

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
    map_sql = f"""
        SELECT DISTINCT slayer, srctablename, tlayer, tgtable
        FROM tgt_dw.META_COLUMNS
        WHERE UPPER(slayer)=UPPER('{RUN_SLAYER}')
          AND UPPER(srctablename)=UPPER('{RUN_SRC_TABLE}')
    """
else:
    raise SystemExit("ERROR: Provide either (RUN_TLAYER & RUN_TGT_TABLE) OR (RUN_SLAYER & RUN_SRC_TABLE).")

mapping = session.sql(map_sql).to_pandas()
if mapping.empty:
    raise SystemExit(f"ERROR: No mapping found for {ident} in META_COLUMNS")
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

# Derive roles (TARGET side uses tgtcolumnname)
pk_cols_tgt     = meta.loc[meta["PRIMARYKEY"].str.upper()=="Y", "TGTCOLUMNNAME"].str.upper().tolist()
ignore_cols_tgt = meta.loc[meta["IGNORE_ROW"].str.upper()=="Y", "TGTCOLUMNNAME"].str.upper().tolist()
use_scd2_cols   = meta.loc[meta["USE_SCD2"].str.upper()=="Y", "TGTCOLUMNNAME"].str.upper().tolist()
is_filter_cols  = meta.loc[meta["IS_FILTER"].str.upper()=="Y", "TGTCOLUMNNAME"].str.upper().tolist()

# Mode (table-level)
mode_vals = meta["HISTORY"].str.upper().unique().tolist()
if len(mode_vals) != 1:
    raise SystemExit("ERROR: Mixed HISTORY flags in META (should be uniform per table).")
MODE = "HISTORY" if mode_vals[0] == "Y" else "SCD2"

# Lifecycle column presence on mapping names (for info)
has_from = (meta["TGTCOLUMNNAME"].str.lower() == "from_date").any()
has_end  = (meta["TGTCOLUMNNAME"].str.lower() == "end_date").any()
has_iscur = "IS_CURRENT" in meta["TGTCOLUMNNAME"].str.upper().tolist()  # optional column in target

from_col = "FROM_DATE" if has_from else None
end_col  = "END_DATE"  if has_end  else None
is_current_col = "IS_CURRENT" if has_iscur else None

# Free-text target filter (first non-null if present)
t_filters = meta["T_FILTER"].dropna().astype(str).str.strip()
t_filter_expr = t_filters.iloc[0] if not t_filters.empty and t_filters.iloc[0] else None

# Select list = all non-ignored target columns (upper-cased)
select_cols_tgt = meta.loc[meta["IGNORE_ROW"].str.upper()!="Y", "TGTCOLUMNNAME"].str.upper().tolist()
select_cols_tgt = list(dict.fromkeys(select_cols_tgt))
if not select_cols_tgt:
    raise SystemExit("ERROR: No selectable target columns (all rows are ignored) in META.")


# ----------------------------------------------------------------------
# Part C) Validate target table & required columns
# ----------------------------------------------------------------------
print("Part C — Validating target table existence and required columns ...")

tbl_chk = session.sql(f"""
SELECT 1
FROM INFORMATION_SCHEMA.TABLES
WHERE UPPER(TABLE_SCHEMA)=UPPER('{TLAYER}')
  AND UPPER(TABLE_NAME)=UPPER('{TGTTABLE}')
""").collect()
if not tbl_chk:
    raise SystemExit(f"ERROR: Target table {TLAYER}.{TGTTABLE} does not exist.")

cols_df = session.sql(f"""
SELECT UPPER(COLUMN_NAME) AS COL
FROM INFORMATION_SCHEMA.COLUMNS
WHERE UPPER(TABLE_SCHEMA)=UPPER('{TLAYER}')
  AND UPPER(TABLE_NAME)=UPPER('{TGTTABLE}')
""").to_pandas()
tgt_cols = set(cols_df["COL"].tolist())

required = set(pk_cols_tgt + select_cols_tgt)
missing  = sorted(required - tgt_cols)
if missing:
    raise SystemExit(f"ERROR: Missing target columns in {TLAYER}.{TGTTABLE}: {missing}")

# Determine how to identify "current/open" versions for SCD2
# Priority: IS_CURRENT='Y' if present; else END_DATE='9999-12-31'; else can't compute "current".
can_identify_open = False
open_predicate = None
if MODE == "SCD2":
    if "IS_CURRENT" in tgt_cols:
        can_identify_open = True
        open_predicate = "UPPER(IS_CURRENT)='Y'"
    elif "END_DATE" in tgt_cols:
        can_identify_open = True
        open_predicate = "END_DATE = DATE '9999-12-31'"
    else:
        print("WARNING: SCD2 mode but no IS_CURRENT or END_DATE in target — will scan full table.")


# ----------------------------------------------------------------------
# Part D) Build filters (t_filter + optional runtime window) & guardrail
# ----------------------------------------------------------------------
print("Part D — Building target WHERE filters ...")

predicates = []

# Apply "current/open" only in SCD2 when we can identify it
if MODE == "SCD2" and can_identify_open:
    predicates.append(f"({open_predicate})")

# Add t_filter (from meta) if provided
if t_filter_expr:
    predicates.append(f"({t_filter_expr})")

# Add runtime window if provided
if WINDOW_PREDICATE:
    predicates.append(f"({WINDOW_PREDICATE})")

if not predicates:
    print("INFO: No target filters applied (scanning target as-is).")

where_sql = f" WHERE {' AND '.join(predicates)}" if predicates else ""
select_list = ", ".join([f'"{c}"' for c in select_cols_tgt])
tgt_sql = f'SELECT {select_list} FROM "{TLAYER}"."{TGTTABLE}"{where_sql}'


# ----------------------------------------------------------------------
# Part E) Materialise TEMP view TGT_SLICE
# ----------------------------------------------------------------------
print("Part E — Materialising TEMP view TGT_SLICE ...")

session.sql("DROP VIEW IF EXISTS TGT_SLICE").collect()
session.sql(f"CREATE TEMP VIEW TGT_SLICE AS {tgt_sql}").collect()


# ----------------------------------------------------------------------
# Part F) Compute stats & print summary
# ----------------------------------------------------------------------
print("Part F — Computing target stats & printing summary ...")

rows_total = session.sql("SELECT COUNT(*) AS C FROM TGT_SLICE").to_pandas()["C"][0]

# PK quality on target slice
if pk_cols_tgt:
    null_pred = " OR ".join([f"{c} IS NULL" for c in pk_cols_tgt])
    null_pk   = session.sql(f"SELECT COUNT(*) AS C FROM TGT_SLICE WHERE {null_pred}").to_pandas()["C"][0]
    pk_list   = ", ".join(pk_cols_tgt)
    dup_pk    = session.sql(f"""
        SELECT COUNT(*) AS C
        FROM (
            SELECT {pk_list}, COUNT(*) AS CNT
            FROM TGT_SLICE
            GROUP BY {pk_list}
            HAVING COUNT(*) > 1
        )
    """).to_pandas()["C"][0]
    distinct_pk = session.sql(f"SELECT COUNT(*) AS C FROM (SELECT DISTINCT {pk_list} FROM TGT_SLICE)").to_pandas()["C"][0]
    if dup_pk > 0:
        raise SystemExit(f"ERROR: Duplicate PKs in target slice for {TLAYER}.{TGTTABLE}. Duplicate groups={dup_pk}")
else:
    null_pk = dup_pk = distinct_pk = 0

# Lifecycle breakdown (best effort)
open_cnt = closed_cnt = None
if MODE == "SCD2":
    if "IS_CURRENT" in tgt_cols:
        open_cnt   = session.sql("SELECT COUNT(*) AS C FROM TGT_SLICE WHERE UPPER(IS_CURRENT)='Y'").to_pandas()["C"][0]
        closed_cnt = rows_total - open_cnt
    elif "END_DATE" in tgt_cols:
        open_cnt   = session.sql("SELECT COUNT(*) AS C FROM TGT_SLICE WHERE END_DATE = DATE '9999-12-31'").to_pandas()["C"][0]
        closed_cnt = rows_total - open_cnt

# Summary print
print("\nSTEP 3 SUMMARY")
print("--------------------------------------------------")
driver = "TARGET-DRIVEN" if (RUN_TLAYER and RUN_TGT_TABLE) else "SOURCE-DRIVEN"
print(f"Driver       : {driver}")
print(f"Target       : {TLAYER}.{TGTTABLE}")
print(f"Mode         : {MODE}")
print(f"Selected cols: {select_cols_tgt}")
print(f"PK cols      : {pk_cols_tgt if pk_cols_tgt else 'NONE'}")
print(f"Rows (total) : {rows_total}")
if pk_cols_tgt:
    print(f"PK distinct  : {distinct_pk} | PK null rows : {null_pk} | PK duplicate groups : {dup_pk}")
if MODE == "SCD2":
    have_iscur = "Y" if "IS_CURRENT" in tgt_cols else "N"
    have_end   = "Y" if "END_DATE" in tgt_cols else "N"
    print(f"Lifecycle    : is_current={have_iscur}, end_date={have_end}")
    if open_cnt is not None:
        print(f"Open/Closed  : OPEN={open_cnt} | CLOSED={closed_cnt}")
if predicates:
    print(f"Filters used : {' AND '.join(predicates)}")
else:
    print("Filters used : NONE")
print("--------------------------------------------------")
print("Successfully completed Step 3.")
