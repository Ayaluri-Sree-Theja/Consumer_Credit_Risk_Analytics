import os
import pandas as pd
import numpy as np

# -----------------------------
# 0) Paths
# -----------------------------
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
ANALYTICS_DIR = "data/analytics"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(ANALYTICS_DIR, exist_ok=True)

# -----------------------------
# 1) Helpers
# -----------------------------
def safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def to_month_start(dt_series: pd.Series) -> pd.Series:
    return dt_series.dt.to_period("M").dt.to_timestamp()

def first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def parse_issue_month(series: pd.Series) -> pd.Series:
    """
    Robust parsing for 'issue_month' which may look like:
    - '2018-01'
    - 'Jan-2018'
    - '2018-01-01'
    """
    s = pd.to_datetime(series, errors="coerce", infer_datetime_format=True)
    # fallback if many NaT
    if s.isna().mean() > 0.25:
        s = pd.to_datetime(series, errors="coerce", format="%b-%Y")
    return s

def process_monthly_series(df: pd.DataFrame, date_col: str, value_col: str, out_name: str,
                           start_month: pd.Timestamp, end_month: pd.Timestamp,
                           agg: str = "mean") -> pd.DataFrame:
    out = df[[date_col, value_col]].copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])

    out["month_start"] = to_month_start(out[date_col])
    out = out[(out["month_start"] >= start_month) & (out["month_start"] <= end_month)]

    if agg == "mean":
        out = out.groupby("month_start", as_index=False)[value_col].mean()
    elif agg == "last":
        out = out.sort_values(date_col).groupby("month_start", as_index=False)[value_col].last()
    else:
        raise ValueError("agg must be 'mean' or 'last'")

    out = out.rename(columns={value_col: out_name})
    return out

# -----------------------------
# 2) Load Raw (untouched)
# -----------------------------
loans_raw = safe_read_csv(os.path.join(RAW_DIR, "loans_full_schema.csv"))
complaints_raw = safe_read_csv(os.path.join(RAW_DIR, "complaints-2025-12-17_01_04.csv"))
unrate_raw = safe_read_csv(os.path.join(RAW_DIR, "UNRATE.csv"))
cpi_raw = safe_read_csv(os.path.join(RAW_DIR, "CPALTT01USM657N.csv"))
dff_raw = safe_read_csv(os.path.join(RAW_DIR, "DFF.csv"))

print("Loaded raw datasets")

# -----------------------------
# 3) Detect Loan Month Window (for macro alignment)
# -----------------------------
if "issue_month" not in loans_raw.columns:
    raise ValueError("loans_full_schema.csv must contain 'issue_month' column (it exists in your earlier output).")

loans_tmp = loans_raw.copy()
loans_tmp["issue_month_dt"] = parse_issue_month(loans_tmp["issue_month"])
loans_tmp["issue_month_start"] = to_month_start(loans_tmp["issue_month_dt"])

loan_min_month = loans_tmp["issue_month_start"].min()
loan_max_month = loans_tmp["issue_month_start"].max()

if pd.isna(loan_min_month) or pd.isna(loan_max_month):
    raise ValueError("Could not parse issue_month into dates. Check loans_raw['issue_month'].head(20).")

print(f"Loan month window detected: {loan_min_month.date()} to {loan_max_month.date()}")

# -----------------------------
# 4) Process Macroeconomic Data (filtered to loan window)
# -----------------------------
# FRED standard uses observation_date + series column
date_col = "observation_date"

unrate_val_col = first_existing_col(unrate_raw, ["UNRATE"])
cpi_val_col = first_existing_col(cpi_raw, ["CPALTT01USM657N"])
dff_val_col = first_existing_col(dff_raw, ["DFF"])

if not unrate_val_col or not cpi_val_col or not dff_val_col:
    raise ValueError("Macro files must include columns: UNRATE, CPALTT01USM657N, DFF (plus observation_date).")

macro_unrate = process_monthly_series(
    unrate_raw, date_col, unrate_val_col, "unemployment_rate",
    loan_min_month, loan_max_month, agg="mean"
)

macro_cpi = process_monthly_series(
    cpi_raw, date_col, cpi_val_col, "cpi_inflation_proxy",
    loan_min_month, loan_max_month, agg="mean"
)

# DFF is daily -> monthly avg (mean)
macro_dff = process_monthly_series(
    dff_raw, date_col, dff_val_col, "fed_funds_rate_avg",
    loan_min_month, loan_max_month, agg="mean"
)

macro_monthly = (
    macro_unrate.merge(macro_cpi, on="month_start", how="left")
               .merge(macro_dff, on="month_start", how="left")
)

print("\n===== MACRO COVERAGE CHECK (after filtering) =====")
print(macro_monthly)
print("\nMissing % by macro column:")
print((macro_monthly.isna().mean() * 100).round(2))

macro_monthly.to_csv(os.path.join(PROCESSED_DIR, "macro_monthly_processed.csv"), index=False)
macro_monthly.to_csv(os.path.join(ANALYTICS_DIR, "fact_macro_monthly.csv"), index=False)
print("Saved macro_monthly_processed.csv and fact_macro_monthly.csv")

# -----------------------------
# 5) Process Loans (clean + features)
# -----------------------------
loans = loans_raw.copy()

# Parse issue_month and create join key
loans["issue_month_dt"] = parse_issue_month(loans["issue_month"])
loans["issue_month_start"] = to_month_start(loans["issue_month_dt"])

# Exclude out-of-scope / low-quality fields from analytics layer (do NOT touch raw)
DROP_LOANS_ANALYTICS = [
    "annual_income_joint",
    "verification_income_joint",
    "debt_to_income_joint",
]
for col in DROP_LOANS_ANALYTICS:
    if col in loans.columns:
        loans.drop(columns=[col], inplace=True)

# Normalize common numeric fields (light-touch sanity guards)
annual_inc_col = first_existing_col(loans, ["annual_income", "annual_inc"])
int_rate_col = first_existing_col(loans, ["interest_rate", "int_rate"])
loan_amt_col = first_existing_col(loans, ["loan_amount", "loan_amnt"])

if annual_inc_col:
    loans.loc[loans[annual_inc_col] <= 0, annual_inc_col] = np.nan

if int_rate_col:
    loans.loc[(loans[int_rate_col] < 0) | (loans[int_rate_col] > 100), int_rate_col] = np.nan

if loan_amt_col:
    loans.loc[loans[loan_amt_col] <= 0, loan_amt_col] = np.nan

# --- Risk Segmentation (Grade proxy)
GRADE_ORDER = ["A", "B", "C", "D", "E", "F", "G"]
grade_to_num = {g: i + 1 for i, g in enumerate(GRADE_ORDER)}  # A=1 ... G=7

if "grade" in loans.columns:
    loans["grade_clean"] = loans["grade"].astype(str).str.strip().str.upper()
    loans["grade_score"] = loans["grade_clean"].map(grade_to_num)

    loans["risk_band"] = np.select(
        [
            loans["grade_clean"].isin(["A", "B"]),
            loans["grade_clean"].isin(["C"]),
            loans["grade_clean"].isin(["D", "E", "F", "G"]),
        ],
        ["Low", "Medium", "High"],
        default="Unknown"
    )
    loans["high_risk_flag"] = (loans["risk_band"] == "High").astype(int)
else:
    loans["risk_band"] = "Unknown"
    loans["high_risk_flag"] = 0

# --- Behavioral Risk Flag (interpretable)
behavior_cols = [
    "months_since_last_delinqu",
    "months_since_90d_late",
    "num_accounts_120d_past_due",
    "num_historical_failed_to_pay",
    "account_never_delinqu_percent",
]
for c in behavior_cols:
    if c not in loans.columns:
        loans[c] = np.nan

loans["behavioral_risk_flag"] = (
    (loans["num_accounts_120d_past_due"].fillna(0) > 0) |
    (loans["num_historical_failed_to_pay"].fillna(0) > 0) |
    (loans["months_since_90d_late"].fillna(999) < 24) |
    (loans["months_since_last_delinqu"].fillna(999) < 24)
).astype(int)

# --- Income banding (quantiles) using individual income only
if annual_inc_col:
    try:
        loans["income_band"] = pd.qcut(
            loans[annual_inc_col],
            q=4,
            labels=["Low", "Lower-Mid", "Upper-Mid", "High"]
        )
    except ValueError:
        bins = [0, 40000, 80000, 120000, np.inf]
        labels = ["Low", "Lower-Mid", "Upper-Mid", "High"]
        loans["income_band"] = pd.cut(loans[annual_inc_col], bins=bins, labels=labels)

# --- Employment length bucket (your emp_length is numeric float in your earlier output)
if "emp_length" in loans.columns:
    loans["emp_length_bucket"] = pd.cut(
        loans["emp_length"],
        bins=[-np.inf, 0, 2, 5, 10, np.inf],
        labels=["0 or less", "1-2", "3-5", "6-10", "10+"]
    )

# --- Term bucket
if "term" in loans.columns:
    loans["term_bucket"] = loans["term"].astype(str).str.replace(" months", "", regex=False)

# --- Join macro onto loans (monthly)
loans = loans.merge(macro_monthly, left_on="issue_month_start", right_on="month_start", how="left")

# Save processed loans
loans.to_csv(os.path.join(PROCESSED_DIR, "loans_processed.csv"), index=False)
print("Saved processed loans: data/processed/loans_processed.csv")

# -----------------------------
# 6) Process Complaints
# -----------------------------
complaints = complaints_raw.copy()

# Standardize column names to snake_case for processed/analytics
rename_map = {
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Company": "company",
    "State": "state",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely_response",
    "Consumer disputed?": "consumer_disputed",
    "Complaint ID": "complaint_id",
    "Consumer consent provided?": "consumer_consent_provided",
    "Company public response": "company_public_response",
    "Tags": "tags",
    "ZIP code": "zip_code",
    "Consumer complaint narrative": "complaint_narrative",
}
complaints.rename(columns={k: v for k, v in rename_map.items() if k in complaints.columns}, inplace=True)

# Parse dates
for dc in ["date_received", "date_sent_to_company"]:
    if dc in complaints.columns:
        complaints[dc] = pd.to_datetime(complaints[dc], errors="coerce")

# Save processed complaints (keep narrative/tags in processed, but drop from analytics facts)
complaints.to_csv(os.path.join(PROCESSED_DIR, "complaints_processed.csv"), index=False)
print("Saved processed complaints: data/processed/complaints_processed.csv")

# Build complaint analytics facts (monthly)
complaints_analytics = complaints.copy()
DROP_COMPLAINTS_ANALYTICS = ["complaint_narrative", "tags", "zip_code"]
complaints_analytics = complaints_analytics.drop(columns=[c for c in DROP_COMPLAINTS_ANALYTICS if c in complaints_analytics.columns])

if "date_received" in complaints_analytics.columns:
    complaints_analytics["month_start"] = to_month_start(complaints_analytics["date_received"])

id_col = "complaint_id" if "complaint_id" in complaints_analytics.columns else None

# Monthly totals
if id_col:
    complaints_monthly = complaints_analytics.groupby("month_start", as_index=False).agg(
        complaints_count=(id_col, "count")
    )
else:
    complaints_monthly = complaints_analytics.groupby("month_start", as_index=False).size().rename(columns={"size": "complaints_count"})

# Monthly by product
if "product" in complaints_analytics.columns and id_col:
    complaints_by_product_month = complaints_analytics.groupby(["month_start", "product"], as_index=False).agg(
        complaints_count=(id_col, "count")
    )
elif "product" in complaints_analytics.columns:
    complaints_by_product_month = complaints_analytics.groupby(["month_start", "product"], as_index=False).size().rename(columns={"size": "complaints_count"})
else:
    complaints_by_product_month = pd.DataFrame(columns=["month_start", "product", "complaints_count"])

complaints_monthly.to_csv(os.path.join(ANALYTICS_DIR, "fact_complaints_monthly.csv"), index=False)
complaints_by_product_month.to_csv(os.path.join(ANALYTICS_DIR, "fact_complaints_by_product_month.csv"), index=False)

print("Saved analytics complaints facts:")
print("   - data/analytics/fact_complaints_monthly.csv")
print("   - data/analytics/fact_complaints_by_product_month.csv")

# -----------------------------
# 7) Build Analytics Tables (Dims + Facts)
# -----------------------------

# DIM: time (month grain)
time_months = pd.Series(pd.to_datetime([]))
for s in [
    loans["issue_month_start"] if "issue_month_start" in loans.columns else pd.Series([]),
    macro_monthly["month_start"] if "month_start" in macro_monthly.columns else pd.Series([]),
    complaints_monthly["month_start"] if "month_start" in complaints_monthly.columns else pd.Series([]),
]:
    s = pd.to_datetime(s, errors="coerce")
    time_months = pd.concat([time_months, s])

dim_time = pd.DataFrame({"month_start": time_months.dropna().drop_duplicates().sort_values()})
dim_time["year"] = dim_time["month_start"].dt.year
dim_time["month"] = dim_time["month_start"].dt.month
dim_time["quarter"] = dim_time["month_start"].dt.to_period("Q").astype(str)

dim_time.to_csv(os.path.join(ANALYTICS_DIR, "dim_time.csv"), index=False)

# DIM: borrower segment (no borrower_id → segment key)
borrower_cols = [c for c in ["homeownership", "verified_income", "income_band", "emp_length_bucket", "state"] if c in loans.columns]
dim_borrower_segment = loans[borrower_cols].drop_duplicates().reset_index(drop=True).copy()
dim_borrower_segment["borrower_segment_id"] = np.arange(1, len(dim_borrower_segment) + 1)
dim_borrower_segment.to_csv(os.path.join(ANALYTICS_DIR, "dim_borrower_segment.csv"), index=False)

# DIM: loan product
product_cols = [c for c in ["loan_purpose", "term", "term_bucket", "grade", "sub_grade", "disbursement_method", "application_type"] if c in loans.columns]
dim_loan_product = loans[product_cols].drop_duplicates().reset_index(drop=True).copy()
dim_loan_product["loan_product_id"] = np.arange(1, len(dim_loan_product) + 1)
dim_loan_product.to_csv(os.path.join(ANALYTICS_DIR, "dim_loan_product.csv"), index=False)

# FACT: loans (SQL-ready)
fact_cols = []
for c in [
    "issue_month_start",
    loan_amt_col,
    int_rate_col,
    "installment",
    "balance",
    "loan_status",
    "high_risk_flag",
    "behavioral_risk_flag",
    "risk_band",
    "income_band",
    "emp_length_bucket",
    "term",
    "term_bucket",
    "loan_purpose",
    "grade",
    "sub_grade",
    "homeownership",
    "verified_income",
    "debt_to_income",
    "unemployment_rate",
    "cpi_inflation_proxy",
    "fed_funds_rate_avg",
]:
    if c and c in loans.columns:
        fact_cols.append(c)

fact_loans = loans[fact_cols].copy()

# Rename amount/rate columns into standard names for BI friendliness
rename_fact = {}
if loan_amt_col and loan_amt_col in fact_loans.columns:
    rename_fact[loan_amt_col] = "loan_amount"
if int_rate_col and int_rate_col in fact_loans.columns:
    rename_fact[int_rate_col] = "interest_rate"
fact_loans.rename(columns=rename_fact, inplace=True)

fact_loans.to_csv(os.path.join(ANALYTICS_DIR, "fact_loans.csv"), index=False)

print("Saved analytics tables:")
print("   - data/analytics/dim_time.csv")
print("   - data/analytics/dim_borrower_segment.csv")
print("   - data/analytics/dim_loan_product.csv")
print("   - data/analytics/fact_loans.csv")
print("   - data/analytics/fact_macro_monthly.csv")

# -----------------------------
# 8) Final Sanity Check (macro join must not be 100% null)
# -----------------------------
macro_cols = ["unemployment_rate", "cpi_inflation_proxy", "fed_funds_rate_avg"]
present_cols = [c for c in macro_cols if c in fact_loans.columns]
if present_cols:
    null_pct = (fact_loans[present_cols].isna().mean() * 100).round(2)
    print("\n===== SANITY CHECK: Macro columns null % in fact_loans =====")
    print(null_pct)
else:
    print("\nMacro columns not found in fact_loans; check merge keys and macro_monthly.")

