snowflake_data_ingestion_step_4.py

# === STEP 4: Classification & Diff (meta-driven) ===
# Sub-parts:
#   A) Inputs & prechecks (uses SRC_SLICE & TGT_SLICE from Steps 2 & 3)
#   B) Load META and derive join & compare columns
#   C) Build FULL OUTER JOIN on PKs (source↔target) using mapped names
#   D) Classify: NEW / CHANGED / UNCHANGED / DELETED
#   E) Summarize counts; optionally show per-row DIFF_COLS when SHOW_DETAILS='Y'

from snowflake.snowpark import Session
import pandas as pd

# -------- Notebook toggle (global for this step) --------
SHOW_DETAILS = "N"  # set to "Y" to include DIFF_COLS for CHANGED rows

# -------- Assumptions carried from Steps 2 & 3 --------
# - Temp views: SRC_SLICE, TGT_SLICE exist
# - Variables TLAYER, TGTTABLE are set (mapping context)
assert "session" in globals(), "ERROR: Snowflake session not found."
assert (
    "TLAYER" in globals() and "TGTTABLE" in globals()
), "ERROR: TLAYER/TGTTABLE not found. Run Steps 2 & 3 first."

print("Part A — Validating prerequisites (temp views, meta presence) ...")

# Check temp views exist
views_ok = session.sql("""
SELECT COUNT(*) AS C FROM INFORMATION_SCHEMA.VIEWS
WHERE UPPER(TABLE_SCHEMA) = CURRENT_SCHEMA()
  AND UPPER(TABLE_NAME) IN ('SRC_SLICE','TGT_SLICE')
""").to_pandas()["C"][0]
if views_ok < 2:
    raise SystemExit(
        "ERROR: Expected TEMP views SRC_SLICE and TGT_SLICE. Run Steps 2 and 3 first."
    )

# Load META for this mapping
meta_sql = f"""
SELECT
  UPPER(srccolumnname) AS SRC_COL,
  UPPER(tgtcolumnname) AS TGT_COL,
  UPPER(primarykey)    AS IS_PK,
  UPPER(ignore_row)    AS IGNORE_ROW,
  UPPER(use_scd2)      AS USE_SCD2
FROM {TLAYER}.META_COLUMNS
WHERE UPPER(tlayer)=UPPER('{TLAYER}')
  AND UPPER(tgtable)=UPPER('{TGTTABLE}')
"""
meta = session.sql(meta_sql).to_pandas()
if meta.empty:
    raise SystemExit(f"ERROR: No META rows for mapping (target) {TLAYER}.{TGTTABLE}")

print("Part B — Deriving join keys and compare columns from META ...")

# PK mapping pairs (source PK -> target PK)
pk_pairs = (
    meta.loc[meta["IS_PK"] == "Y", ["SRC_COL", "TGT_COL"]]
    .drop_duplicates()
    .values.tolist()
)
if not pk_pairs:
    raise SystemExit("ERROR: No primary key columns in META for this table.")

# Compare pairs: use only SCD2 attrs that are not ignored
cmp_pairs = (
    meta[(meta["USE_SCD2"] == "Y") & (meta["IGNORE_ROW"] != "Y")][
        ["SRC_COL", "TGT_COL"]
    ]
    .drop_duplicates()
    .values.tolist()
)
# (If none, we can still do NEW/DELETED/UNCHANGED based on presence)
# For safety, exclude PKs from compare (no need to diff PKs)
pk_srcs = {p[0] for p in pk_pairs}
cmp_pairs = [p for p in cmp_pairs if p[0] not in pk_srcs]

print(f"  PK pairs   : {pk_pairs}")
print(
    f"  Compare on : {cmp_pairs if cmp_pairs else 'NONE (only NEW/DELETED/UNCHANGED detection)'}"
)
print(f"  Details    : {'ON' if SHOW_DETAILS.upper()=='Y' else 'OFF'}")

print(
    "Part C — Building FULL OUTER JOIN between SRC_SLICE and TGT_SLICE on mapped PKs ..."
)

# Build join predicate: s.src_pk = t.tgt_pk AND ...
join_pred = " AND ".join([f"s.{src} = t.{tgt}" for src, tgt in pk_pairs])

# Build selection of PKs (coalesce across sides to ensure a single PK output per row)
pk_select = ",\n    ".join(
    [f"COALESCE(s.{src}, t.{tgt}) AS {tgt}" for src, tgt in pk_pairs]
)

# Build diff conditions across compare columns
if cmp_pairs:
    diff_or = " OR ".join(
        [
            f"(s.{s} <> t.{t} OR (s.{s} IS NULL AND t.{t} IS NOT NULL) OR (s.{s} IS NOT NULL AND t.{t} IS NULL))"
            for s, t in cmp_pairs
        ]
    )
else:
    diff_or = "FALSE"

print("Part D — Classifying rows (NEW / CHANGED / UNCHANGED / DELETED) ...")

# Choose a representative PK for presence tests (first PK pair)
rep_src_pk, rep_tgt_pk = pk_pairs[0]

# Optional DIFF_COLS array when SHOW_DETAILS='Y'
diff_cols_expr = ""
if SHOW_DETAILS.upper() == "Y" and cmp_pairs:
    diffs = [
        f"CASE WHEN (s.{s} <> t.{t} OR (s.{s} IS NULL AND t.{t} IS NOT NULL) OR (s.{s} IS NOT NULL AND t.{t} IS NULL)) THEN '{t}' END"
        for s, t in cmp_pairs
    ]  # report DIFF names using target column names
    diff_cols_expr = (
        ",\n    ARRAY_CONSTRUCT_COMPACT(" + ", ".join(diffs) + ") AS DIFF_COLS"
    )

classification_sql = f"""
CREATE OR REPLACE TEMP VIEW CLASSIFIED AS
SELECT
    {pk_select},
    CASE
      WHEN s.{rep_src_pk} IS NOT NULL AND t.{rep_tgt_pk} IS NULL THEN 'NEW'
      WHEN s.{rep_src_pk} IS NULL     AND t.{rep_tgt_pk} IS NOT NULL THEN 'DELETED'
      WHEN s.{rep_src_pk} IS NOT NULL AND t.{rep_tgt_pk} IS NOT NULL AND ({diff_or}) THEN 'CHANGED'
      WHEN s.{rep_src_pk} IS NOT NULL AND t.{rep_tgt_pk} IS NOT NULL THEN 'UNCHANGED'
    END AS CLASSIFICATION
    {diff_cols_expr}
FROM SRC_SLICE s
FULL OUTER JOIN TGT_SLICE t
  ON {join_pred}
"""
session.sql(classification_sql).collect()

print("Part E — Summarizing results ...")

summary = session.sql("""
  SELECT CLASSIFICATION, COUNT(*) AS CNT
  FROM CLASSIFIED
  GROUP BY CLASSIFICATION
  ORDER BY CLASSIFICATION
""").to_pandas()

print("\nSTEP 4 SUMMARY")
print("--------------------------------------------------")
for _, row in summary.iterrows():
    print(f"{row['CLASSIFICATION']:<10} : {int(row['CNT'])}")
if SHOW_DETAILS.upper() == "Y" and cmp_pairs:
    sample = session.sql("""
      SELECT * FROM CLASSIFIED
      WHERE CLASSIFICATION = 'CHANGED'
      LIMIT 20
    """).to_pandas()
    if not sample.empty:
        print("\nSample CHANGED rows (showing DIFF_COLS):")
        print(sample)
print("--------------------------------------------------")
print("Successfully completed Step 4.")
