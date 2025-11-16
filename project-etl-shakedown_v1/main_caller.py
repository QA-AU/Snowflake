# =========================================================
# main_caller.py
# User-Facing Entry Points for Table Shakedown Framework
# =========================================================

from typing import Optional, List, Dict
from engine.orchestrator import run_shakedown
from auto_runner import auto_runner


# =========================================================
# Single Table Caller
# =========================================================
def run_single_table(
        session,
        table_fqn: str,
        business_date: str,
        debug: bool = False):
    """
    Run the full shakedown for a single specified table.

    Parameters
    ----------
    session : Snowpark Session
    table_fqn : "DB.SCHEMA.TABLE"
    business_date : "YYYY-MM-DD"
    debug : bool

    Returns
    -------
    DataFrame
        Results from STG.QA_SHAKEDOWN_RESULTS
    """
    if debug:
        print(f"[MAIN] Running SINGLE shakedown on {table_fqn}")
        print(f"[MAIN] Business date: {business_date}")

    return run_shakedown(
        session=session,
        table_fqn=table_fqn,
        business_date=business_date,
        debug=debug
    )


# =========================================================
# Auto-Runner Caller
# =========================================================
def run_all_tables(
        session,
        business_date: str,
        tables_filter: Optional[List[str]] = None,
        debug: bool = False) -> Dict[str, "DataFrame"]:
    """
    Auto-discover all metadata JSON files in @temp_config_stage
    and run shakedown tests for each table.

    Parameters
    ----------
    session : Snowpark Session
    business_date : "YYYY-MM-DD"
    tables_filter : Optional[List[str]]
        If provided, only metadata files with names containing 
        any of the filter strings will run.
        Example: ["ORDER", "CUSTOMER"]
    debug : bool

    Returns
    -------
    Dict[str, DataFrame]
        Key = table_fqn
        Value = results DataFrame
    """

    if debug:
        print("[MAIN] Running AUTO mode for all metadata JSON files")
        print(f"[MAIN] Business date: {business_date}")
        if tables_filter:
            print(f"[MAIN] Filter applied: {tables_filter}")

    return auto_runner(
        session=session,
        business_date=business_date,
        tables_filter=tables_filter,
        debug=debug
    )


# =========================================================
# Convenience Main Caller
# =========================================================
def main_caller(
        session,
        mode: str,
        business_date: str,
        table_fqn: Optional[str] = None,
        tables_filter: Optional[List[str]] = None,
        debug: bool = False):
    """
    Unified calling interface for the entire framework.

    Parameters
    ----------
    session : Snowpark Session
    mode : "single" or "auto"
    business_date : "YYYY-MM-DD"
    table_fqn : Required for mode="single"
    tables_filter : Optional list for mode="auto"
    debug : bool

    Example Usage:
        main_caller(session, "single",
                    business_date="2025-02-01",
                    table_fqn="CORE.SALES.ORDER_DIM")

        main_caller(session, "auto",
                    business_date="2025-02-01",
                    tables_filter=["ORDER"],
                    debug=True)
    """

    mode = mode.lower().strip()

    if mode not in ("single", "auto"):
        raise ValueError("mode must be either 'single' or 'auto'")

    if mode == "single":
        if not table_fqn:
            raise ValueError("table_fqn is required in mode='single'")

        return run_single_table(
            session=session,
            table_fqn=table_fqn,
            business_date=business_date,
            debug=debug
        )

    else:  # mode == "auto"
        return run_all_tables(
            session=session,
            business_date=business_date,
            tables_filter=tables_filter,
            debug=debug
        )
