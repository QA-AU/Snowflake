CREATE OR REPLACE PROCEDURE STG.DIAG_READ_CSV_DELIM_PY(
    stage_path STRING,
    delim_char STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark import Session

def run(session: Session, stage_path: str, delim_char: str) -> str:

    if not delim_char or len(delim_char) != 1:
        raise ValueError("Delimiter must be a single character")

    # Read staged file as text
    df = session.read.text(stage_path)

    # ONLY sample a few rows — no full scan
    sample_df = df.limit(5)

    rows = sample_df.collect()
    if not rows:
        return "FAIL: File readable but no data returned"

    split_rows = [
        row["VALUE"].split(delim_char) for row in rows
    ]

    max_columns = max(len(cols) for cols in split_rows)

    return (
        "SUCCESS (SAMPLED ONLY)\n"
        f"Delimiter used    : '{delim_char}'\n"
        f"Rows sampled      : {len(rows)}\n"
        f"Max columns found : {max_columns}\n"
        f"Sample split rows : {split_rows}"
    )
$$;



CALL STG.DIAG_READ_CSV_DELIM_PY(
  '@DB.SCHEMA.STAGE/path/file.csv',
  ','
);

CALL STG.DIAG_READ_CSV_DELIM_PY(
  '@DB.SCHEMA.STAGE/path/file.csv',
  '|'
);

CALL STG.DIAG_READ_CSV_DELIM_PY(
  '@DB.SCHEMA.STAGE/path/file.csv',
  '\t'
);

