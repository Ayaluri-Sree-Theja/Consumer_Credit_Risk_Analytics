CREATE OR REPLACE VIEW vw_risk_by_segment AS
SELECT
  l.issue_month_start AS month_start,
  COALESCE(l.income_band, 'Unknown') AS income_band,
  COALESCE(l.emp_length_bucket, 'Unknown') AS emp_length_bucket,
  COALESCE(l.homeownership, 'Unknown') AS homeownership,
  COALESCE(l.verified_income, 'Unknown') AS verified_income,
  COALESCE(l.grade, 'Unknown') AS grade,

  COUNT(*) AS loan_count,
  SUM(l.loan_amount) AS exposure,
  ROUND(AVG(l.interest_rate), 3) AS avg_interest_rate,
  ROUND(AVG(l.high_risk_flag), 4) AS high_risk_rate,
  ROUND(AVG(l.behavioral_risk_flag), 4) AS behavioral_risk_rate,
  SUM(CASE WHEN l.high_risk_flag = 1 THEN l.loan_amount ELSE 0 END) AS high_risk_exposure
FROM fact_loans l
GROUP BY
  l.issue_month_start,
  COALESCE(l.income_band, 'Unknown'),
  COALESCE(l.emp_length_bucket, 'Unknown'),
  COALESCE(l.homeownership, 'Unknown'),
  COALESCE(l.verified_income, 'Unknown'),
  COALESCE(l.grade, 'Unknown');
