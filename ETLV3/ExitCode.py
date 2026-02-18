
## 📂  Final Structure Will Be:
```
-repo/
├── Snowflake/
│   ├── engine_v2.py
│   ├── metadata_loader.py
│   ├── ( existing files...)
│   └── ETLV3/                           ← NEW
│       ├── phase1_parser.py             ← NEW
│       ├── Phase1_DataInput.xlsx        ← NEW
│       ├── VERSION_INFO.md              ← NEW
│       ├── EXIT_CODES.md                ← NEW
│       ├── CHANGES_SUMMARY.md           ← NEW
│       ├── PHASE1_CONVERSATION_SUMMARY.md ← NEW
│       └── phase1_output_sample.json    ← NEW


# Phase 1 Parser — Exit Codes & Status Messages

## 📊 Exit Codes

The parser uses standard Unix exit codes:

| Exit Code | Meaning | When |
|-----------|---------|------|
| **0** | Success | Validation passed, JSON generated |
| **1** | Failure | File not found, validation errors, or other exceptions |

---

## ✅ Success Output

### Example:
```bash
$ python phase1_parser.py -i Phase1_DataInput.xlsx -o mapping.json
```

### Console Output:
```
════════════════════════════════════════════════════════════
  Building SQL for mapping: MAP_001
════════════════════════════════════════════════════════════

[SELECT]
SELECT
    T1.PKCOL1 AS COL_1,
    T1.PKCOL2 AS RLTD_COL_1,
    ...

[FROM]
FROM TEST_1_DB.S_SCHEMA.DEMO_TABLE T1 LEFT JOIN ...

[WHERE]
WHERE
      $order_date between STRT_DT and END_DT

[QUALIFY]
QUALIFY ROW_NUMBER() OVER (ORDER BY T1.PKCOL1, T1.PKCOL2, T1.RELKIND) <= 100

[FULL SQL]
SELECT ...
────────────────────────────────────────────────────────────

======================================================================
  ✅ PHASE 1 COMPLETE — SUCCESS
======================================================================
  Mappings processed: 1
  Output location:    mapping.json
  Warnings:           0
  Status:             PASSED
  Exit code:          0
======================================================================
```

### Check Exit Code:
```bash
$ echo $?
0
```

---

## ⚠️  Success with Warnings

Warnings are **non-blocking** — the parser still exits with code 0.

### Example Output:
```
⚠️  WARNINGS (non-blocking):
  Mapping 1: Single source table but source_join is populated — verify intentional.
  COLUMN_MAPPING row 5: source_column_override is populated but transformationtype=COPY — did you mean SQL?

======================================================================
  ✅ PHASE 1 COMPLETE — SUCCESS
======================================================================
  Mappings processed: 1
  Output location:    mapping.json
  Warnings:           2
  Status:             PASSED
  Exit code:          0
======================================================================
```

**Exit code:** 0 ✅

---

## ❌ Failure: File Not Found

### Example:
```bash
$ python phase1_parser.py -i missing.xlsx
```

### Console Output:
```
======================================================================
  ❌ PHASE 1 FAILED — FILE NOT FOUND
======================================================================
  File path:   missing.xlsx
  Status:      FAILED
  Exit code:   1
  Action:      Check file path and try again
======================================================================
```

**Exit code:** 1 ❌

---

## ❌ Failure: Validation Errors

### Example:
```bash
$ python phase1_parser.py -i bad_mapping.xlsx
```

### Console Output:
```
❌ VALIDATION FAILED:

  Mapping 1: 'mapping_id' is mandatory but empty.
  Mapping 1: Multiple source tables detected but source_join is empty.
  COLUMN_MAPPING row 5: 'source_column' is mandatory but empty.
  COLUMN_MAPPING row 7: Duplicate target_column 'COL_1' in mapping 'MAP_001'.

======================================================================
  ❌ PHASE 1 FAILED — VALIDATION ERRORS
======================================================================
  Status:      FAILED
  Exit code:   1
  Action:      Fix validation errors above and re-run
======================================================================
```

**Exit code:** 1 ❌

---

## 🔄 Using Exit Codes in Scripts

### Bash Script Example:
```bash
#!/bin/bash

python phase1_parser.py -i input.xlsx -o output.json

if [ $? -eq 0 ]; then
    echo "✅ Parsing successful, proceeding to Phase 2..."
    python phase2_processor.py -i output.json
else
    echo "❌ Parsing failed, stopping pipeline"
    exit 1
fi
```

### CI/CD Pipeline Example:
```yaml
- name: Run Phase 1 Parser
  run: python phase1_parser.py -i mappings.xlsx -o output.json
  # Will fail the CI job if exit code is 1
```

### Python Script Example:
```python
import subprocess
import sys

result = subprocess.run(
    ["python", "phase1_parser.py", "-i", "input.xlsx", "-o", "output.json"],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("✅ Parsing successful")
    print(result.stderr)  # Status summary
else:
    print("❌ Parsing failed")
    print(result.stderr)  # Error details
    sys.exit(1)
```

---

## 📝 Message Components

### Success Summary Contains:
- ✅ Visual indicator (green checkmark)
- Total mappings processed
- Output file location (or "stdout")
- Warning count
- Status: PASSED
- Exit code: 0

### Failure Summary Contains:
- ❌ Visual indicator (red X)
- Failure reason (FILE NOT FOUND or VALIDATION ERRORS)
- Error details (before the summary box)
- Status: FAILED
- Exit code: 1
- Action: What to do next

---

## 🎯 Best Practices

### 1. Always Check Exit Codes in Automation
```bash
python phase1_parser.py -i input.xlsx -o output.json || exit 1
```

### 2. Redirect stderr to Capture Status
```bash
python phase1_parser.py -i input.xlsx 2> status.log
```

### 3. Silent Mode (Suppress SQL Generation)
```bash
python phase1_parser.py -i input.xlsx -o output.json > /dev/null
# Still shows status summary to stderr
```

### 4. Log Everything
```bash
python phase1_parser.py -i input.xlsx -o output.json 2>&1 | tee full.log
```

---

## 🔍 Debugging Failed Runs

### Step 1: Check the Exit Code
```bash
echo $?  # After running the parser
```

### Step 2: Review Error Messages
All errors print to **stderr** before the failure summary.

### Step 3: Common Failures & Fixes

| Error | Fix |
|-------|-----|
| File not found | Check file path, ensure .xlsx extension |
| Mandatory field empty | Fill all fields marked with (*) in Excel |
| Multi-table without join | Add source_join clause for multiple tables |
| Duplicate mapping_id | Make mapping_id unique |
| Invalid dedup_logic | Must be KEEP_FIRST, KEEP_LAST, or REJECT |
| transformationtype=SQL without rule | Add transformationrule expression |

---

## 📊 Exit Code Summary Table

| Scenario | Errors | Warnings | Exit Code | Status |
|----------|--------|----------|-----------|--------|
| Perfect run | 0 | 0 | **0** | ✅ PASSED |
| Run with warnings | 0 | >0 | **0** | ✅ PASSED |
| File not found | 1 | 0 | **1** | ❌ FAILED |
| Validation errors | >0 | any | **1** | ❌ FAILED |

**Key Point:** Warnings do not cause failure. Only errors do.

---

*Exit codes and messages added in v1.0.0 (2026-02-18)*