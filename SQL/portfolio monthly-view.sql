CREATE OR REPLACE VIEW vw_portfolio_monthly AS
SELECT
  l.issue_month_start AS month_start,
  YEAR(l.issue_month_start) AS year,
  MONTH(l.issue_month_start) AS month,

  COUNT(*) AS loan_count,
  SUM(l.loan_amount) AS total_exposure,

  SUM(CASE WHEN l.high_risk_flag = 1 THEN l.loan_amount ELSE 0 END) AS high_risk_exposure,
  ROUND(
    SUM(CASE WHEN l.high_risk_flag = 1 THEN l.loan_amount ELSE 0 END)
    / NULLIF(SUM(l.loan_amount), 0),
    4
  ) AS high_risk_exposure_pct,

  ROUND(AVG(l.interest_rate), 3) AS avg_interest_rate,
  ROUND(AVG(l.behavioral_risk_flag), 4) AS behavioral_risk_rate,

  AVG(m.unemployment_rate) AS unemployment_rate,
  AVG(m.cpi_inflation_proxy) AS cpi_proxy,
  AVG(m.fed_funds_rate_avg) AS fed_funds_rate
FROM fact_loans l
LEFT JOIN fact_macro_monthly m
  ON m.month_start = l.issue_month_start
GROUP BY l.issue_month_start;
