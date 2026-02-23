snowflake_data_ingestion_step_7.py

# === FINAL PASS/FAIL FLAG ===
PASS = (
    (sim_cnt == tgt_cnt)
    and (len(missing_in_tgt) == 0)
    and (len(extra_in_tgt) == 0)
    and (("diff_cnt" in locals() and diff_cnt == 0))
)

if PASS:
    print(" VALIDATION PASSED — Target matches simulated post-state.")
else:
    print(" VALIDATION FAILED — Differences detected. Review summary above.")
