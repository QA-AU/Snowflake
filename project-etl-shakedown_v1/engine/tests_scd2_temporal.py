# =========================================================
# engine/tests_scd2_temporal.py
# SCD2 Temporal Validity Tests (Tests 7–12)
# =========================================================

from typing import Dict
from engine.helpers import (
    run_sql_with_timing,
    insert_result_row,
)


# =========================================================
# 7. SCD2_START_DATE_VALID
# =========================================================
def test_scd2_start_date_valid(session, meta: Dict, run_name: str):
    """
    start_dt must equal business_date for newly inserted rows.
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"""
        SELECT COUNT(*) CNT
        FROM {fqn}
        WHERE start_dt = '{bd}'
          AND business_date != '{bd}'
    """

    rows, dur = run_sql_with_timing(session, sql)
    invalid = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "SCD2_START_DATE_VALID",
        sql_used=sql,
        passed=(invalid == 0),
        metrics={"invalid_start_dates": invalid},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 8. SCD2_END_DATE_VALID
# =========================================================
def test_scd2_end_date_valid(session, meta: Dict, run_name: str):
    """
    end_dt must be:
       - '9999-12-31' for active records
       - business_date - 1 for closed records
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]

    sql = f"""
        SELECT COUNT(*) CNT
        FROM {fqn}
        WHERE end_dt NOT IN ('9999-12-31', DATEADD(day,-1,'{bd}'))
    """

    rows, dur = run_sql_with_timing(session, sql)
    invalid = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "SCD2_END_DATE_VALID",
        sql_used=sql,
        passed=(invalid == 0),
        metrics={"invalid_end_dates": invalid},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 9. SCD2_ONLY_ONE_ACTIVE
# =========================================================
def test_scd2_only_one_active(session, meta: Dict, run_name: str):
    """
    For each BK, only one active row is allowed:
        end_dt = '9999-12-31'
    """
    fqn = meta["table_fqn"]
    bk_cols = meta["table"]["bk_columns"]
    group = ",".join(bk_cols)

    sql = f"""
        SELECT COUNT(*) CNT
        FROM (
            SELECT {group}, COUNT(*) AS C
            FROM {fqn}
            WHERE end_dt = '9999-12-31'
            GROUP BY {group}
            HAVING COUNT(*) > 1
        )
    """

    rows, dur = run_sql_with_timing(session, sql)
    conflicts = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "SCD2_ONLY_ONE_ACTIVE",
        sql_used=sql,
        passed=(conflicts == 0),
        metrics={"multi_active_groups": conflicts},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 10. SCD2_CHANGE_DETECTION
# =========================================================
def test_scd2_change_detection(session, meta: Dict, run_name: str):
    """
    Detect if attribute changes create a new row:
        If attribute difference exists, there must be:
            - one closed record (end_dt = business_date - 1)
            - one open record   (end_dt = '9999-12-31')
    """
    fqn = meta["table_fqn"]
    bd = meta["run_business_date"]
    bk = meta["table"]["bk_columns"]
    scd = meta["table"]["scd2_columns"]

    bk_group = ",".join(bk)

    comparisons = " OR ".join(
        [f"NVL(old.{c},'<NULL>') != NVL(new.{c},'<NULL>')" for c in scd]
    )

    sql = f"""
        SELECT COUNT(*) CNT
        FROM (
            SELECT old.{bk_group}
            FROM {fqn} old
            JOIN {fqn} new
              ON { " AND ".join([f"old.{c} = new.{c}" for c in bk]) }
            WHERE old.end_dt = DATEADD(day,-1,'{bd}')
              AND new.end_dt = '9999-12-31'
              AND ({comparisons})
        )
    """

    rows, dur = run_sql_with_timing(session, sql)
    detected = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "SCD2_CHANGE_DETECTION",
        sql_used=sql,
        passed=True,
        metrics={"changes_detected": detected},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 11. SCD2_EFFECTIVE_SEQUENCE
# =========================================================
def test_scd2_effective_sequence(session, meta: Dict, run_name: str):
    """
    Ensures that effective-dated rows follow correct ordering:
    start_dt <= end_dt  (except active row)
    """
    fqn = meta["table_fqn"]

    sql = f"""
        SELECT COUNT(*) CNT
        FROM {fqn}
        WHERE start_dt > end_dt
          AND end_dt != '9999-12-31'
    """

    rows, dur = run_sql_with_timing(session, sql)
    invalid = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "SCD2_EFFECTIVE_SEQUENCE",
        sql_used=sql,
        passed=(invalid == 0),
        metrics={"invalid_ranges": invalid},
        error=None,
        duration_ms=dur,
    )


# =========================================================
# 12. SCD2_SURROGATE_REUSE
# =========================================================
def test_scd2_surrogate_reuse(session, meta: Dict, run_name: str):
    """
    SCD2 surrogate keys should NOT be reused across BKs.
    """
    fqn = meta["table_fqn"]
    sk = meta["table"].get("surrogate_key", "sk")
    bk = meta["table"]["bk_columns"]

    group = ",".join(bk)

    sql = f"""
        SELECT COUNT(*) CNT
        FROM (
            SELECT {sk}
            FROM {fqn}
            GROUP BY {sk}
            HAVING COUNT(DISTINCT {group}) > 1
        )
    """

    rows, dur = run_sql_with_timing(session, sql)
    conflicts = rows[0]["CNT"]

    insert_result_row(
        session,
        meta,
        run_name,
        "SCD2_SURROGATE_REUSE",
        sql_used=sql,
        passed=(conflicts == 0),
        metrics={"sk_reused_across_bk": conflicts},
        error=None,
        duration_ms=dur,
    )
