quick_scd2_test_sql_notebook.py

# === SCD2 VALIDATOR — NOTEBOOK PYTHON WRAPPER ===
# What this cell does:
#   1) (Optional) samples N PKs from the SOURCE to limit scope, else validates full tables
#   2) Builds helper temp views (SRC_K, TGT_K) for the chosen key set
#   3) Runs SCD2 invariants as separate SQL checks
#   4) Aggregates results into a single pandas table: [check_name, violations, status]
#
# Assumptions:
#   - SOURCE and TARGET share the SAME natural key column names (PKs). If not, adjust the join expressions.
#   - TARGET has SCD2 columns: FROM_DATE, END_DATE; optionally IS_CURRENT.
#
# Inputs to edit:
SRC_SCHEMA      = "SRC_STG"
SRC_TABLE       = "CUSTOMER_STG"
TGT_SCHEMA      = "TGT_DW"
TGT_TABLE       = "DIM_CUSTOMER"

# Natural key columns (list of strings) — same names in SRC and TGT
PK_COLS         = ["CUSTOMER_ID"]

# Open-row criterion: use IS_CURRENT='Y' if present, else END_DATE = HIGH_DATE
USE_IS_CURRENT  = True           # if False → use END_DATE == HIGH_DATE
HIGH_DATE       = "9999-12-31"   # high date literal used in your SCD2

# Limit validation to a sample of PKs from SOURCE (set to None to validate full tables)
SAMPLE_PK_COUNT = 1000           # e.g., 1000 PKs; or set to None for full scan

# Optional extra filter on the source to choose the window (e.g., last_load_date)
SRC_FILTER      = None           # e.g., "load_date >= '2025-08-01'"

# ============== Do not edit below ==============
import pandas as pd
from snowflake.snowpark import Session

assert 'session' in globals(), "Snowflake `session` not found. Ensure the Notebook created a Snowpark session."

SRC_FQN = f'"{SRC_SCHEMA}"."{SRC_TABLE}"'
TGT_FQN = f'"{TGT_SCHEMA}"."{TGT_TABLE}"'
PK_CSV  = ", ".join([f'"{c}"' for c in PK_COLS])
PK_EQ   = " AND ".join([f's."{c}" = t."{c}"' for c in PK_COLS])

print("=== SCD2 VALIDATOR: setup keys ===")
# 0) Choose key set: sample or full
session.sql("DROP VIEW IF EXISTS SRC_KEYS").collect()
src_sel = f"SELECT {PK_CSV} FROM {SRC_FQN}"
if SRC_FILTER:
    src_sel += f" WHERE {SRC_FILTER}"
if SAMPLE_PK_COUNT:
    # DISTINCT keys then LIMIT
    src_keys_sql = f'CREATE TEMP VIEW SRC_KEYS AS SELECT DISTINCT {PK_CSV} FROM ({src_sel}) LIMIT {int(SAMPLE_PK_COUNT)}'
    scope_note = f"SAMPLED: {SAMPLE_PK_COUNT} PKs from source"
else:
    src_keys_sql = f'CREATE TEMP VIEW SRC_KEYS AS SELECT DISTINCT {PK_CSV} FROM ({src_sel})'
    scope_note = "FULL: all PKs from source (may be heavy)"
session.sql(src_keys_sql).collect()

# Target key view for the same PK set (intersection by PKs)
session.sql("DROP VIEW IF EXISTS TGT_KEYS").collect()
tgt_keys_sql = f"""
CREATE TEMP VIEW TGT_KEYS AS
SELECT t.{PK_CSV}
FROM {TGT_FQN} t
JOIN SRC_KEYS s ON {" AND ".join([f's."{c}"=t."{c}"' for c in PK_COLS])}
GROUP BY {PK_CSV}
"""
session.sql(tgt_keys_sql).collect()

# Determine open/current predicate
if USE_IS_CURRENT:
    OPEN_PRED = "UPPER(IS_CURRENT)='Y'"
else:
    OPEN_PRED = f"END_DATE = DATE '{HIGH_DATE}'"

print(f"Scope        : {scope_note}")
print(f"Open rows    : {OPEN_PRED}")

# 1) Check: inverted ranges (FROM_DATE > END_DATE) — within target rows for sampled PKs
inv_sql = f"""
SELECT COUNT(*) AS VIOLATIONS
FROM {TGT_FQN} t
JOIN TGT_KEYS k ON {" AND ".join([f'k."{c}"=t."{c}"' for c in PK_COLS])}
WHERE t.FROM_DATE > t.END_DATE
"""

# 2) Check: exactly one current row per PK
curr_sql = f"""
SELECT COUNT(*) AS VIOLATIONS
FROM (
  SELECT {PK_CSV}, COUNT(*) AS c
  FROM {TGT_FQN} t
  JOIN TGT_KEYS k ON {" AND ".join([f'k."{c}"=t."{c}"' for c in PK_COLS])}
  WHERE {OPEN_PRED}
  GROUP BY {PK_CSV}
  HAVING COUNT(*) <> 1
)
"""

# 3) Check: overlapping ranges per PK (pairwise intersect)
# Build PK equality once
PK_EQ_A_B = " AND ".join([f'a.\"{c}\"=b.\"{c}\"' for c in PK_COLS])
overlap_sql = f"""
SELECT COUNT(*) AS VIOLATIONS
FROM {TGT_FQN} a
JOIN {TGT_FQN} b
  ON {PK_EQ_A_B}
JOIN TGT_KEYS k ON {" AND ".join([f'k."{c}"=a."{c}"' for c in PK_COLS])}
WHERE (a.FROM_DATE <= b.END_DATE AND b.FROM_DATE <= a.END_DATE)
  AND (a.SYSTEM$ROW_ID() <> b.SYSTEM$ROW_ID())
"""

# 4) Check: coverage — every SRC key must map to an open/current row in target
cov_sql = f"""
SELECT COUNT(*) AS VIOLATIONS
FROM SRC_KEYS s
LEFT JOIN (
  SELECT {PK_CSV}
  FROM {TGT_FQN}
  WHERE {OPEN_PRED}
  GROUP BY {PK_CSV}
) t
ON {PK_EQ}
WHERE { " OR ".join([f't.\"{c}\" IS NULL' for c in PK_COLS])}
"""

# 5) Check: orphan closed rows — closed row not followed by next_from = end_date + 1
part_cols = PK_CSV
orphan_sql = f"""
WITH x AS (
  SELECT {part_cols}, FROM_DATE, END_DATE,
         LEAD(FROM_DATE) OVER (PARTITION BY {part_cols} ORDER BY FROM_DATE) AS next_from
  FROM {TGT_FQN} t
  JOIN TGT_KEYS k ON {" AND ".join([f'k."{c}"=t."{c}"' for c in PK_COLS])}
)
SELECT COUNT(*) AS VIOLATIONS
FROM x
WHERE END_DATE <> DATE '{HIGH_DATE}'
  AND (next_from IS NULL OR next_from <> DATEADD(DAY,1,END_DATE))
"""

# 6) Info: gaps (non-fatal) — same as 5 but only count gaps, not missing successors
gaps_sql = f"""
WITH x AS (
  SELECT {part_cols}, FROM_DATE, END_DATE,
         LEAD(FROM_DATE) OVER (PARTITION BY {part_cols} ORDER BY FROM_DATE) AS next_from
  FROM {TGT_FQN} t
  JOIN TGT_KEYS k ON {" AND ".join([f'k."{c}"=t."{c}"' for c in PK_COLS])}
)
SELECT COUNT(*) AS VIOLATIONS
FROM x
WHERE END_DATE <> DATE '{HIGH_DATE}'
  AND next_from IS NOT NULL
  AND next_from <> DATEADD(DAY,1,END_DATE)
"""

# Run checks
def one_val(sql):
    return int(session.sql(sql).to_pandas().iloc[0,0])

results = []
checks = [
    ("1) Inverted ranges (FROM_DATE > END_DATE)", inv_sql, True),
    ("2) Exactly one current row per PK",         curr_sql, True),
    ("3) Overlapping date ranges per PK",         overlap_sql, True),
    ("4) Source coverage missing in current",     cov_sql, True),
    ("5) Orphan closed rows (no next contiguous)",orphan_sql, True),
    ("6) Gaps in date chains (info)",             gaps_sql, False),
]

for name, sql, strict in checks:
    v = one_val(sql)
    status = ("PASS" if v==0 else ("FAIL" if strict else "WARN"))
    results.append({"check_name": name, "violations": v, "status": status})

df = pd.DataFrame(results)
display(df)  # In Snowflake Notebook: renders a nice table

# Overall PASS/FAIL (strict on checks 1–5; 6 is informational)
overall_ok = all((r["violations"]==0) for r in results if r["check_name"].startswith(tuple(str(i)+")" for i in range(1,6))))
print("\n=== OVERALL ===")
print(f"Scope: {scope_note}")
print(f"Source: {SRC_FQN}  |  Target: {TGT_FQN}")
print("Open criterion:", OPEN_PRED)
print("Result:", "✅ VALIDATION PASSED" if overall_ok else "❌ VALIDATION FAILED")
