CREATE OR REPLACE VIEW vw_complaints_monthly AS
SELECT
  month_start,
  complaints_count,
  complaints_count - LAG(complaints_count) OVER (ORDER BY month_start) AS mom_change,
  ROUND(
    (complaints_count - LAG(complaints_count) OVER (ORDER BY month_start)) /
    NULLIF(LAG(complaints_count) OVER (ORDER BY month_start), 0),
    4
  ) AS mom_growth_rate
FROM fact_complaints_monthly;
