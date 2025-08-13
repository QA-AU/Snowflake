-- sqls to test tables - No transpose  - tested on 13/08/25

--------------Start ---------
-- Replace table and column names as needed
WITH src AS (
    SELECT *
    FROM SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.customers 
),
tgt AS (
    SELECT *
    FROM SFQAZIAZAM_LOAD_SAMPLE_DATA_FROM_S3.dim_customer
),

-- Rows present in SRC but not in TGT
only_in_src AS (
    SELECT * FROM src
    MINUS
    SELECT * FROM tgt
),

-- Rows present in TGT but not in SRC
only_in_tgt AS (
    SELECT * FROM tgt
    MINUS
    SELECT * FROM src
),

-- Pick one PK from any diff
---USER - to change/add/rename id/pk column 
diff_pk AS (
    SELECT id FROM only_in_src
    UNION
    SELECT id FROM only_in_tgt
    LIMIT 1
)

SELECT 'source' , s.* 
FROM src s
JOIN diff_pk d ON s.id = d.id 
Union all
SELECT 'target' , t.*
FROM tgt t
JOIN diff_pk d ON t.id = d.id ;
--- USER to modify joins above as required. 


-----------END-------------------


---
