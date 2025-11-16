# =========================================================
# auto_runner.py
# Auto-discovery Runner for Full Table Shakedown Suite
# =========================================================

from typing import List, Optional
from metadata_loader import load_metadata
from engine.orchestrator import run_shakedown
import json


# =========================================================
# Helper: list metadata JSON files from @temp_config_stage
# =========================================================
def _list_metadata_files(session) -> List[str]:
    """
    Lists all JSON metadata files in @temp_config_stage.
    Returns filenames such as: ["ORDER_DIM.json", "CUSTOMER_DIM.json"]
    """
    rows = session.sql("LIST @temp_config_stage").collect()
    files = [
        row["name"].split("/")[-1]
        for row in rows
        if row["name"].lower().endswith(".json")
    ]
    return files


# =========================================================
# Helper: extract table FQN from metadata JSON file
# =========================================================
def _extract_table_fqn_from_metadata(session, json_path: str) -> str:
    """
    Loads the JSON metadata and returns table_fqn.
    The metadata must contain "table_fqn" or user provides this externally.
    """
    rows = session.sql(f"SELECT $1 FROM @temp_config_stage/{json_path}").collect()
    meta = json.loads(rows[0]["$1"])

    if "table" not in meta:
        raise KeyError(f"Metadata {json_path} missing 'table' section.")

    # If user explicitly included table_fqn inside metadata
    if "table_fqn" in meta:
        return meta["table_fqn"]

    # Else infer from JSON filename
    # e.g., SALES_ORDER_DIM.json -> SALES.ORDER_DIM (schema.table)
    # User must override if needed.
    filename = json_path.replace(".json", "")
    if "_" in filename:
        schema, table = filename.split("_", 1)
        return f"{schema}.{table}"
    else:
        # No schema detected — user will specify DB/SHEMA separately
        return filename  # Fallback: TABLE only


# =========================================================
# Auto-Runner
# =========================================================
def auto_runner(session,
                business_date: str,
                tables_filter: Optional[List[str]] = None,
                debug: bool = False):
    """
    Automatically discovers *all* metadata JSON files in @temp_config_stage
    and runs shakedown tests for each table.

    Parameters
    ----------
    session : Snowpark Session
    business_date : str  (YYYY-MM-DD)
    tables_filter : Optional[List[str]]
        Limit the run to a subset of tables (by name).
    debug : bool
        If True, prints diagnostic execution trace.

    Returns
    -------
    Dict[str, DataFrame]
        Key = table_fqn
        Value = result DataFrame for that table
    """

    if debug:
        print("[DEBUG] Starting auto_runner")
        print(f"[DEBUG] Business date: {business_date}")

    # -----------------------------------------------------
    # 1. Discover all metadata JSON files
    # -----------------------------------------------------
    metadata_files = _list_metadata_files(session)

    if debug:
        print("[DEBUG] Metadata files discovered:", metadata_files)

    if tables_filter:
        metadata_files = [
            f for f in metadata_files
            if any(t.lower() in f.lower() for t in tables_filter)
        ]
        if debug:
            print("[DEBUG] Filtered metadata files:", metadata_files)

    results = {}

    # -----------------------------------------------------
    # 2. Loop over each JSON file and run shakedown
    # -----------------------------------------------------
    for json_file in metadata_files:

        try:
            # Infer table_fqn or load from metadata_content
            table_fqn = _extract_table_fqn_from_metadata(session, json_file)

            if debug:
                print(f"[DEBUG] Running shakedown for table: {table_fqn}")

            # Main single-table shakedown call
            df = run_shakedown(
                session=session,
                table_fqn=table_fqn,
                business_date=business_date,
                debug=debug
            )

            results[table_fqn] = df

        except Exception as ex:
            if debug:
                print(f"[ERROR] Auto-run failed for file: {json_file}")
                print("[ERROR] Reason:", str(ex))
            # Continue with next file

    return results
