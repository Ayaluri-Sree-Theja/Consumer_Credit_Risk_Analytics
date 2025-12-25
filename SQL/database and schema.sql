-- 0) Create & use schema
CREATE DATABASE IF NOT EXISTS credit_risk_analytics;
USE credit_risk_analytics;

-- 1) Dimensions
DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time (
  month_start DATE PRIMARY KEY,
  year INT,
  month INT,
  quarter VARCHAR(6)
);

DROP TABLE IF EXISTS dim_borrower_segment;
CREATE TABLE dim_borrower_segment (
  borrower_segment_id INT PRIMARY KEY,
  homeownership VARCHAR(50),
  verified_income VARCHAR(50),
  income_band VARCHAR(20),
  emp_length_bucket VARCHAR(20),
  state VARCHAR(10)
);

DROP TABLE IF EXISTS dim_loan_product;
CREATE TABLE dim_loan_product (
  loan_product_id INT PRIMARY KEY,
  loan_purpose VARCHAR(100),
  term VARCHAR(20),
  term_bucket VARCHAR(10),
  grade VARCHAR(5),
  sub_grade VARCHAR(5),
  disbursement_method VARCHAR(50),
  application_type VARCHAR(50)
);

-- 2) Facts
DROP TABLE IF EXISTS fact_macro_monthly;
CREATE TABLE fact_macro_monthly (
  month_start DATE PRIMARY KEY,
  unemployment_rate DECIMAL(6,3),
  cpi_inflation_proxy DECIMAL(10,4),
  fed_funds_rate_avg DECIMAL(6,3),
  INDEX idx_macro_month (month_start)
);

DROP TABLE IF EXISTS fact_complaints_monthly;
CREATE TABLE fact_complaints_monthly (
  month_start DATE PRIMARY KEY,
  complaints_count INT,
  INDEX idx_cmp_month (month_start)
);

DROP TABLE IF EXISTS fact_complaints_by_product_month;
CREATE TABLE fact_complaints_by_product_month (
  month_start DATE,
  product VARCHAR(100),
  complaints_count INT,
  PRIMARY KEY (month_start, product),
  INDEX idx_cmp_prod (product)
);

DROP TABLE IF EXISTS fact_loans;
CREATE TABLE fact_loans (
  issue_month_start DATE,
  loan_amount DECIMAL(14,2),
  interest_rate DECIMAL(6,3),
  installment DECIMAL(14,2),
  balance DECIMAL(14,2),
  loan_status VARCHAR(50),

  high_risk_flag TINYINT,
  behavioral_risk_flag TINYINT,
  risk_band VARCHAR(20),

  income_band VARCHAR(20),
  emp_length_bucket VARCHAR(20),

  term VARCHAR(20),
  term_bucket VARCHAR(10),
  loan_purpose VARCHAR(100),
  grade VARCHAR(5),
  sub_grade VARCHAR(5),

  homeownership VARCHAR(50),
  verified_income VARCHAR(50),
  debt_to_income DECIMAL(8,3),

  unemployment_rate DECIMAL(6,3),
  cpi_inflation_proxy DECIMAL(10,4),
  fed_funds_rate_avg DECIMAL(6,3),

  INDEX idx_loans_month (issue_month_start),
  INDEX idx_loans_status (loan_status),
  INDEX idx_loans_grade (grade),
  INDEX idx_loans_purpose (loan_purpose)
);
