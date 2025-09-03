CREATE OR REPLACE PROCEDURE sp_scd2_merge_v1(
    SRC_DB STRING,
    SRC_SCHEMA STRING,
    SRC_TABLE STRING,
    TGT_DB STRING,
    TGT_SCHEMA STRING,
    TGT_TABLE STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    pk_cols STRING;
    merge_cols STRING;
    sql_stmt STRING;
BEGIN
    -- 1. Get PK columns from target table
    LET pk_cols = (
        SELECT LISTAGG(column_name, ', ')
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE table_schema = TGT_SCHEMA
          AND table_name = TGT_TABLE
          AND constraint_name IN (
              SELECT constraint_name
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
              WHERE table_schema = TGT_SCHEMA
                AND table_name = TGT_TABLE
                AND constraint_type = 'PRIMARY KEY'
          )
    );

    -- 2. Get candidate merge columns (exclude PK + SCD2 audit columns)
    LET merge_cols = (
        SELECT LISTAGG(column_name, ', ')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = TGT_SCHEMA
          AND table_name = TGT_TABLE
          AND upper(column_name) NOT IN (
              SELECT upper(column_name)
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE table_schema = TGT_SCHEMA
                AND table_name = TGT_TABLE
          )
          AND upper(column_name) NOT IN ('VALID_FROM','VALID_TO','IS_CURRENT','HASH_DIFF')
    );

    -- 3. Build dynamic MERGE SQL
    LET sql_stmt = '
        MERGE INTO ' || TGT_DB || '.' || TGT_SCHEMA || '.' || TGT_TABLE || ' tgt
        USING ' || SRC_DB || '.' || SRC_SCHEMA || '.' || SRC_TABLE || ' src
        ON ' || LISTAGG('tgt.' || pk || ' = src.' || pk, ' AND ') 
            OVER (SELECT SPLIT_TO_TABLE(pk_cols, ' ,') pk) || '
           AND tgt.is_current = TRUE

        WHEN MATCHED AND (HASH(src.' || merge_cols || ') <> tgt.hash_diff) THEN
            UPDATE SET tgt.is_current = FALSE,
                       tgt.valid_to = CURRENT_DATE

        WHEN NOT MATCHED THEN
            INSERT (' || pk_cols || ', ' || merge_cols || ', valid_from, valid_to, is_current, hash_diff)
            VALUES (' || LISTAGG('src.' || pk, ', ') OVER (SELECT SPLIT_TO_TABLE(pk_cols, ' ,') pk)
                   || ', ' || merge_cols || ', CURRENT_DATE, ''9999-12-31'', TRUE, HASH(' || merge_cols || '))
    ';

    -- 4. Execute
    EXECUTE IMMEDIATE sql_stmt;

    RETURN 'SCD2 MERGE completed for ' || TGT_SCHEMA || '.' || TGT_TABLE;
END;
$$;

CALL sp_scd2_merge_v1(
  'STAGING_DB', 'STG', 'CUSTOMER_STG',
  'DW_DB', 'DIM', 'CUSTOMER_DIM'
);

