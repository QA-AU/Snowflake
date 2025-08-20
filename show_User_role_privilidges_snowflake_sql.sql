show_User_role_privilidges_snowflake_sql.py


-- Username + role/object privileges for ALL users in the account
WITH RECURSIVE user_roles AS (
  SELECT
    GRANTEE_NAME AS username,
    ROLE         AS role_name
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
  WHERE DELETED_ON IS NULL
),
role_tree AS (
  SELECT username, role_name
  FROM user_roles
  UNION ALL
  SELECT t.username, g.NAME
  FROM role_tree t
  JOIN SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES g
    ON g.GRANTEE_NAME = t.role_name
   AND g.GRANTED_ON   = 'ROLE'
   AND g.DELETED_ON   IS NULL
),
all_roles AS (
  SELECT DISTINCT username, role_name
  FROM role_tree
),
role_privs AS (
  SELECT
    GRANTEE_NAME AS role_name,
    PRIVILEGE,
    GRANTED_ON,
    TABLE_CATALOG AS database_name,
    TABLE_SCHEMA  AS schema_name,
    NAME          AS object_name
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
  WHERE DELETED_ON IS NULL
    AND GRANTED_ON IN ('DATABASE','SCHEMA','TABLE','VIEW','MATERIALIZED VIEW','WAREHOUSE')
)
SELECT
  a.username,
  a.role_name,
  p.granted_on,
  p.database_name,
  p.schema_name,
  p.object_name,
  p.privilege
FROM all_roles a
LEFT JOIN role_privs p
  ON p.role_name = a.role_name
ORDER BY a.username, a.role_name, p.database_name, p.schema_name, p.object_name, p.privilege;