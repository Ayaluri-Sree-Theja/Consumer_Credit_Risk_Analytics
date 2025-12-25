CREATE OR REPLACE VIEW vw_risk_by_product AS
SELECT
  l.issue_month_start AS month_start,
  COALESCE(l.loan_purpose, 'Unknown') AS loan_purpose,
  COALESCE(l.term_bucket, 'Unknown') AS term_bucket,
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
  COALESCE(l.loan_purpose, 'Unknown'),
  COALESCE(l.term_bucket, 'Unknown'),
  COALESCE(l.grade, 'Unknown');
