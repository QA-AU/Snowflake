-- Assume your raw table is T1(ID, STRT_DT, END_DT) stored as DATE
-- If END_DT uses 9/09/9999 for “open”, we normalize it to 9999-12-31.

WITH norm AS (
  SELECT
      ID,
      STRT_DT::DATE                                          AS s,
      CASE
        WHEN END_DT IN ('9999-09-09'::DATE, '2099-09-09'::DATE) THEN '9999-12-31'::DATE
        ELSE END_DT::DATE
      END                                                    AS e
  FROM T1
),

-- Build change events: +1 at start, -1 at day after end
events AS (
  SELECT ID, s                           AS d,  1  AS delta FROM norm
  UNION ALL
  SELECT ID, DATEADD(day, 1, e)          AS d, -1  AS delta FROM norm
),

-- Collapse same-day events (important if multiple rows start/end on same day)
collapsed AS (
  SELECT ID, d, SUM(delta) AS delta
  FROM events
  GROUP BY ID, d
),

-- Running active-count over time for each ID
running AS (
  SELECT
      ID,
      d,
      SUM(delta) OVER (PARTITION BY ID ORDER BY d
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS active
  FROM collapsed
),

-- Convert change points into closed intervals [start_dt, end_dt]
spans AS (
  SELECT
      ID,
      d                                                               AS start_dt,
      COALESCE(DATEADD(day, -1,
               LEAD(d) OVER (PARTITION BY ID ORDER BY d)), '9999-12-31'::DATE) AS end_dt,
      active
  FROM running
)

-- Keep only periods where at least one row was active
SELECT
    ID,
    start_dt AS STRT_DT,
    end_dt   AS END_DT
FROM spans
WHERE active > 0
ORDER BY ID, STRT_DT;

Replace T1 with your table name.
If your dates are VARCHAR like DD/MM/YYYY, cast them first, e.g. TO_DATE(STRT_DT, 'DD/MM/YYYY').
Adjust the open-ended sentinel(s) in the CASE to match what you store (e.g., 9/09/9999).
Output will match your Table-2:
