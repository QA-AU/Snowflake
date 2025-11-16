# =========================================================
# engine/orchestrator.py
# Main Shakedown Orchestrator
# =========================================================

from typing import Dict
from engine.registry import TEST_REGISTRY
from metadata_loader import load_metadata
from engine.helpers import insert_result_row


# =========================================================
# Orchestrator: run_shakedown
# =========================================================
def run_shakedown(session,
                  table_fqn: str,
                  business_date: str,
                  debug: bool = False):
    """
    Main orchestration function for running the complete table shakedown
    against a single table for a given business_date.

    Parameters
    ----------
    session : snowflake.snowpark.Session
    table_fqn : str
        Fully qualified table name to test (DB.SCHEMA.TABLE)
    business_date : str
        Business date for the test run (YYYY-MM-DD)
    debug : bool
        If True, prints debugging output

    Returns
    -------
    DataFrame
        Results of STG.QA_SHAKEDOWN_RESULTS for this run
    """

    # -----------------------------------------------------
    # Load metadata (automatically from @temp_config_stage)
    # -----------------------------------------------------
    meta: Dict = load_metadata(session, table_fqn, debug)
    meta["run_business_date"] = business_date
    meta["table_fqn"] = table_fqn  # ensure set explicitly

    run_name = f"{table_fqn}__{business_date}"

    if debug:
        print("[DEBUG] Metadata loaded:")
        print(meta)
        print(f"[DEBUG] Starting run: {run_name}")

    # -----------------------------------------------------
    # Create results table if not exists
    # -----------------------------------------------------
    session.sql("""
        CREATE TABLE IF NOT EXISTS STG.QA_SHAKEDOWN_RESULTS (
            RUN_NAME STRING,
            TABLE_FQN STRING,
            TEST_NAME STRING,
            SQL_USED STRING,
            PASS_FLAG BOOLEAN,
            METRICS STRING,
            ERROR STRING,
            DURATION_MS NUMBER
        )
    """).collect()

    # -----------------------------------------------------
    # Execute each test
    # -----------------------------------------------------
    tests_to_run = meta.get("tests_to_run", [])
    if debug:
        print(f"[DEBUG] Tests to run: {tests_to_run}")

    for test_name in tests_to_run:

        # Validate existence in registry
        if test_name not in TEST_REGISTRY:
            insert_result_row(
                session, meta, run_name, test_name,
                sql_used="",
                passed=False,
                metrics={},
                error=f"Test '{test_name}' not found in registry.",
                duration_ms=0
            )
            if debug:
                print(f"[ERROR] Test '{test_name}' not found in registry.")
            continue

        if debug:
            print(f"[DEBUG] Running test: {test_name}")

        test_func = TEST_REGISTRY[test_name]

        # -----------------------------------------
        # Execute test with exception protection
        # -----------------------------------------
        try:
            test_func(session, meta, run_name)

        except Exception as ex:
            err = str(ex)

            insert_result_row(
                session, meta, run_name, test_name,
                sql_used="",
                passed=False,
                metrics={},
                error=err,
                duration_ms=0
            )

            if debug:
                print(f"[ERROR] Test '{test_name}' failed with exception: {err}")
            continue

    # -----------------------------------------------------
    # Return result set for this run
    # -----------------------------------------------------
    df = session.sql(f"""
        SELECT *
        FROM STG.QA_SHAKEDOWN_RESULTS
        WHERE RUN_NAME = '{run_name}'
        ORDER BY TEST_NAME
    """)

    if debug:
        print("[DEBUG] Shakedown complete.")

    return df
