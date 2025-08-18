snowflake_data_ingestion_step_6.py

# === STEP 6: Compare SIM output vs actual target ===
# Sub-parts:
#   A) Column alignment check
#   B) Row-count comparison
#   C) PK-level symmetric diff (missing/excess)
#   D) Value-level mismatches on intersecting PKs (sample)

from snowflake.snowpark import Session
import pandas as pd

# Uses VALIDATION_SCHEMA and SIM_TABLE from Step 5
assert 'session' in globals(), "ERROR: Snowflake session not found."
assert 'VALIDATION_SCHEMA' in globals() and 'SIM_TABLE' in globals(), "ERROR: SIM config not found. Run Step 5."

print("Part A — Checking column alignment ...")
sim_cols = session.sql(f"""
  SELECT UPPER(COLUMN_NAME) AS COL
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE UPPER(TABLE_SCHEMA)=UPPER('{VALIDATION_SCHEMA}')
    AND UPPER(TABLE_NAME)=UPPER('{SIM_TABLE}')
""").to_pandas()["COL"].tolist()

tgt_cols = session.sql(f"""
  SELECT UPPER(COLUMN_NAME) AS COL
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE UPPER(TABLE_SCHEMA)=UPPER('{TLAYER}')
    AND UPPER(TABLE_NAME)=UPPER('{TGTTABLE}')
""").to_pandas()["COL"].tolist()

# Alignment = intersection for compare (ignore extra cols)
common_cols = [c for c in sim_cols if c in set(tgt_cols)]
if not common_cols:
    raise SystemExit("ERROR: No common columns between SIM and target.")
cols_csv = ", ".join([f'"{c}"' for c in common_cols])

print("Part B — Row-count compare ...")
sim_cnt = session.sql(f'SELECT COUNT(*) C FROM {VALIDATION_SCHEMA}."{SIM_TABLE}"').to_pandas()["C"][0]
tgt_cnt = session.sql(f'SELECT COUNT(*) C FROM "{TLAYER}"."{TGTTABLE}"').to_pandas()["C"][0]
print(f"  SIM rows: {sim_cnt} | TARGET rows: {tgt_cnt}")

print("Part C — PK-level symmetric diff ...")
# Get PKs from META
pk_tgts = session.sql(f"""
  SELECT UPPER(tgtcolumnname) AS PK
  FROM {TLAYER}.META_COLUMNS
  WHERE UPPER(tlayer)=UPPER('{TLAYER}')
    AND UPPER(tgtable)=UPPER('{TGTTABLE}')
    AND UPPER(primarykey)='Y'
""").to_pandas()["PK"].tolist()
if not pk_tgts:
    raise SystemExit("ERROR: No PK columns in META for diffing.")

pk_csv = ", ".join([f'"{k}"' for k in pk_tgts])
session.sql("CREATE OR REPLACE TEMP VIEW SIM_KEYS AS " +
            f"SELECT {pk_csv} FROM {VALIDATION_SCHEMA}.\"{SIM_TABLE}\" GROUP BY {pk_csv}").collect()
session.sql("CREATE OR REPLACE TEMP VIEW TGT_KEYS AS " +
            f"SELECT {pk_csv} FROM \"{TLAYER}\".\"{TGTTABLE}\" GROUP BY {pk_csv}").collect()

# Missing in target / extra in target
missing_in_tgt = session.sql(f"""
  SELECT s.* FROM SIM_KEYS s
  LEFT JOIN TGT_KEYS t ON {" AND ".join([f"s.\"{k}\"=t.\"{k}\"" for k in pk_tgts])}
  WHERE { " AND ".join([f"t.\"{k}\" IS NULL" for k in pk_tgts])}
""").to_pandas()
extra_in_tgt = session.sql(f"""
  SELECT t.* FROM TGT_KEYS t
  LEFT JOIN SIM_KEYS s ON {" AND ".join([f"s.\"{k}\"=t.\"{k}\"" for k in pk_tgts])}
  WHERE { " AND ".join([f"s.\"{k}\" IS NULL" for k in pk_tgts])}
""").to_pandas()

print(f"  Missing in TARGET (PKs): {len(missing_in_tgt)}")
print(f"  Extra in TARGET   (PKs): {len(extra_in_tgt)}")

print("Part D — Value mismatches on intersecting PKs (sample) ...")
# Join SIM vs TARGET on PK; compare common columns
join_on = " AND ".join([f"s.\"{k}\" = t.\"{k}\"" for k in pk_tgts])
diff_or = " OR ".join([f"NVL(s.\"{c}\", '§NULL§') <> NVL(t.\"{c}\", '§NULL§')" for c in common_cols if c not in pk_tgts])
if not diff_or:
    print("  No comparable non-PK columns; skipping value diff.")
else:
    session.sql(f"""
      CREATE OR REPLACE TEMP VIEW V_DIFF AS
      SELECT s.{cols_csv}, t.{cols_csv}
      FROM {VALIDATION_SCHEMA}."{SIM_TABLE}" s
      JOIN "{TLAYER}"."{TGTTABLE}" t ON {join_on}
      WHERE {diff_or}
    """).collect()
    diff_cnt = session.sql("SELECT COUNT(*) C FROM V_DIFF").to_pandas()["C"][0]
    print(f"  Value-mismatch rows: {diff_cnt}")
    if diff_cnt > 0:
        sample = session.sql("SELECT * FROM V_DIFF LIMIT 20").to_pandas()
        print(sample)

print("\nSTEP 6 SUMMARY")
print("--------------------------------------------------")
print(f"Row count     : SIM={sim_cnt} | TARGET={tgt_cnt}")
print(f"PK symm diff  : missing_in_target={len(missing_in_tgt)} | extra_in_target={len(extra_in_tgt)}")
if 'diff_cnt' in locals():
    print(f"Value mismatch: {diff_cnt}")
print("--------------------------------------------------")
print("Successfully completed Step 6 (comparison).")
