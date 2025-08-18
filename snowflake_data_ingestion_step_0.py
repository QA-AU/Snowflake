snowflake_data_ingestion_step_0.py

# === STEP 0 + STEP 1: Metadata load & validation ===
# This step will:
#   1. Read Excel file into a pandas DataFrame
#   2. Validate required headers and flag values
#   3. Check guardrails (PKs, history consistency, SCD2 prerequisites, is_filter uniqueness)
#   4. Create meta_columns table in Snowflake if not exists
#   5. Overwrite meta_columns with validated metadata
#   6. Print per-table summary (PKs, Mode, Lifecycle, Delete col, Filter col)

import pandas as pd
from snowflake.snowpark import Session

# --- CONFIG ---
file_path = "meta_config.xlsx"   # adjust to your uploaded Excel

# 0.1 Read Excel
print("Validating: reading Excel file ...")
df_meta = pd.read_excel(file_path)
df_meta.columns = [c.strip().lower() for c in df_meta.columns]

# 0.2 Required headers
print("Validating: required headers ...")
required = [
    "tlayer","slayer","srctablename","tgtable",
    "srccolumnname","tgtcolumnname",
    "primarykey","ignore_row","use_scd2",
    "is_deleted","is_filter","history"
]
missing = [c for c in required if c not in df_meta.columns]
if missing:
    raise SystemExit(f"ERROR: Missing required column(s): {missing}")

# 0.3 Empty check
print("Validating: Excel not empty ...")
if df_meta.empty:
    raise SystemExit("ERROR: Metadata Excel is empty")

# 0.4 Flag values
print("Validating: flag values ...")
flag_cols = ["primarykey","ignore_row","use_scd2","is_deleted","is_filter","history"]
for col in flag_cols:
    bad = df_meta[~df_meta[col].astype(str).str.upper().isin(["Y","N"])]
    if not bad.empty:
        raise SystemExit(f"ERROR: Invalid flag values in column '{col}': {bad[col].unique().tolist()}")

# Group by table
grouped = df_meta.groupby(["slayer","srctablename","tlayer","tgtable"])

# 1.1 Primary key
print("Validating: primary key per table ...")
for keys, g in grouped:
    if not (g["primarykey"].str.upper() == "Y").any():
        raise SystemExit(f"ERROR: {keys} has no primary key defined")

# 1.2 History consistency
print("Validating: history flag consistency ...")
for keys, g in grouped:
    vals = g["history"].str.upper().unique()
    if len(vals) > 1:
        raise SystemExit(f"ERROR: {keys} has mixed history flags (Y/N)")

# 1.3 SCD2 prerequisites
print("Validating: SCD2 prerequisites ...")
for keys, g in grouped:
    if g["history"].str.upper().iloc[0] == "N":  # SCD2 mode
        from_present = (g["srccolumnname"].str.lower() == "from_date").any()
        end_present  = (g["srccolumnname"].str.lower() == "end_date").any()
        if not (from_present and end_present):
            raise SystemExit(f"ERROR: {keys} missing from_date/end_date mapping for SCD2 mode")
        if (g["is_deleted"].str.upper() == "Y").sum() > 1:
            raise SystemExit(f"ERROR: {keys} has multiple is_deleted=Y columns")

# 1.4 is_filter uniqueness
print("Validating: is_filter uniqueness ...")
for keys, g in grouped:
    if (g["is_filter"].str.upper() == "Y").sum() > 1:
        raise SystemExit(f"ERROR: {keys} has multiple is_filter=Y columns")

# Create table if not exists
print("Creating meta_columns table if not exists ...")
session.sql("""
CREATE TABLE IF NOT EXISTS tgt_dw.meta_columns (
  tlayer STRING, slayer STRING,
  srctablename STRING, tgtable STRING,
  srccolumnname STRING, tgtcolumnname STRING,
  srcdatatype STRING, tgtdatatype STRING,
  srcformat STRING, tgtformat STRING,
  primarykey STRING, ignore_row STRING, use_scd2 STRING,
  from_date STRING, end_date STRING,
  is_deleted STRING,
  s_filter STRING, t_filter STRING,
  is_filter STRING,
  history STRING
)
""").collect()

# Overwrite Snowflake table
print("Overwriting tgt_dw.meta_columns with validated metadata ...")
session.write_pandas(df_meta, "meta_columns", schema="tgt_dw", overwrite=True)

# Final summary
tables = grouped.ngroups
print(f"\nSuccessfully completed Step 0 & 1 | rows={len(df_meta)} | tables={tables}\n")

# Per-table summary
print("TABLE SUMMARY")
print("--------------------------------------------------")
for keys, g in grouped:
    slayer, srctablename, tlayer, tgtable = keys
    pks = g.loc[g["primarykey"].str.upper() == "Y", "srccolumnname"].tolist()
    mode = "HISTORY" if g["history"].str.upper().iloc[0] == "Y" else "SCD2"
    from_present = (g["srccolumnname"].str.lower() == "from_date").any()
    end_present  = (g["srccolumnname"].str.lower() == "end_date").any()
    del_cols = g.loc[g["is_deleted"].str.upper() == "Y", "srccolumnname"].tolist()
    del_col = del_cols[0] if del_cols else "NONE"
    filt_cols = g.loc[g["is_filter"].str.upper() == "Y", "srccolumnname"].tolist()
    filt_col = filt_cols[0] if filt_cols else "NONE"

    print(f"{slayer}.{srctablename} -> {tlayer}.{tgtable}")
    print(f"  PKs       : {pks}")
    print(f"  Mode      : {mode}")
    print(f"  Lifecycle : from_date={'Y' if from_present else 'N'}, "
          f"end_date={'Y' if end_present else 'N'}, delete_col={del_col}")
    print(f"  is_filter : {filt_col}")
    print("--------------------------------------------------")
