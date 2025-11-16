# =========================================================
# metadata_loader.py
# Metadata Loader for Table Shakedown Framework
# =========================================================

import json
from typing import Dict


# =========================================================
# Helper: Load a JSON file from a Snowflake stage
# =========================================================
def _load_json_from_stage(session, stage_path: str) -> Dict:
    """
    Loads a JSON file from a stage using:
        SELECT $1 FROM @stage/file
    Returns Python dictionary.
    """
    rows = session.sql(f"SELECT $1 FROM {stage_path}").collect()
    if not rows:
        raise ValueError(f"No rows returned from stage path: {stage_path}")

    raw_text = rows[0]["$1"]
    return json.loads(raw_text)


# =========================================================
# Helper: Auto-discover metadata JSON from @temp_config_stage
# =========================================================
def _discover_metadata_file(session, table_fqn: str, debug: bool) -> str:
    """
    Discover a JSON file matching the table under test.

    Naming convention options:
        1. SCHEMA_TABLE.json
        2. TABLE.json
        3. table.json

    Example:
        For table CORE.SALES.ORDER_DIM:
            - "ORDER_DIM.json"
            - "SALES_ORDER_DIM.json"
    """
    db, schema, table = table_fqn.split(".")

    candidates = [
        f"{schema}_{table}.json",
        f"{table}.json",
        f"{table.lower()}.json",
    ]

    listing = session.sql("LIST @temp_config_stage").collect()
    files = [row["name"].split("/")[-1] for row in listing]

    if debug:
        print("[DEBUG] Available JSON files in @temp_config_stage:", files)

    for candidate in candidates:
        if candidate in files:
            if debug:
                print(f"[DEBUG] Metadata file selected: {candidate}")
            return f"@temp_config_stage/{candidate}"

    raise FileNotFoundError(
        f"No metadata JSON found for table {table_fqn}. "
        f"Searched for: {candidates} in @temp_config_stage."
    )


# =========================================================
# Metadata Loader (MAIN ENTRY)
# =========================================================
def load_metadata(session, table_fqn: str, debug: bool = False) -> Dict:
    """
    Loads metadata for the given table from @temp_config_stage.

    Steps:
    -----
    1. Auto-discover JSON file
    2. Load JSON into dictionary
    3. Validate required fields
    4. Normalize internal structure
    """
    if debug:
        print(f"[DEBUG] Loading metadata for table: {table_fqn}")

    # -----------------------------------------------------
    # 1. Auto-discover the JSON metadata file
    # -----------------------------------------------------
    stage_path = _discover_metadata_file(session, table_fqn, debug)

    # -----------------------------------------------------
    # 2. Load JSON into Python dictionary
    # -----------------------------------------------------
    meta = _load_json_from_stage(session, stage_path)

    if debug:
        print("[DEBUG] Raw metadata loaded:")
        print(json.dumps(meta, indent=4))

    # -----------------------------------------------------
    # 3. Validate mandatory metadata sections
    # -----------------------------------------------------
    if "table" not in meta:
        raise KeyError("Metadata missing top-level 'table' section.")

    table_meta = meta["table"]

    required_fields = ["bk_columns", "scd2_required_columns"]
    for field in required_fields:
        if field not in table_meta:
            raise KeyError(f"Metadata missing required field: table.{field}")

    # -----------------------------------------------------
    # 4. Add fully qualified table name to metadata
    # -----------------------------------------------------
    meta["table_fqn"] = table_fqn

    # -----------------------------------------------------
    # 5. Ensure tests_to_run exists
    # -----------------------------------------------------
    if "tests_to_run" not in meta:
        raise KeyError("Metadata missing 'tests_to_run' list.")

    # -----------------------------------------------------
    # 6. Version tagging (optional)
    # -----------------------------------------------------
    meta["version"] = meta.get("version", "1.0")

    if debug:
        print("[DEBUG] Final metadata structure:")
        print(json.dumps(meta, indent=4))

    return meta
