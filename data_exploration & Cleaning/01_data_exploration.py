import pandas as pd
import numpy as np

# -----------------------------
# 1. Load Data
# -----------------------------
DATA_PATH = "data/raw/"

loans = pd.read_csv(f"{DATA_PATH}loans_full_schema.csv")
complaints = pd.read_csv(f"{DATA_PATH}complaints-2025-12-17_01_04.csv")
unrate = pd.read_csv(f"{DATA_PATH}UNRATE.csv")
cpi = pd.read_csv(f"{DATA_PATH}CPALTT01USM657N.csv")
rates = pd.read_csv(f"{DATA_PATH}DFF.csv")

print("Datasets loaded successfully\n")

# -----------------------------
# 2. Dataset Inventory
# -----------------------------
def dataset_inventory(df, name):
    print(f"===== {name.upper()} DATASET =====")
    print(f"Shape: {df.shape}")
    print(f"Columns: {len(df.columns)}")
    print("Sample rows:")
    print(df.head(3))
    print("\n")

dataset_inventory(loans, "Loans")
dataset_inventory(complaints, "Complaints")
dataset_inventory(unrate, "Unemployment Rate")
dataset_inventory(cpi, "Inflation (CPI)")
dataset_inventory(rates, "Federal Funds Rate")

# -----------------------------
# 3. Column Profiling (Loans)
# -----------------------------
print("===== LOANS COLUMN PROFILING =====")
profile = (
    loans
    .isna()
    .mean()
    .reset_index()
    .rename(columns={"index": "column", 0: "missing_pct"})
)
profile["missing_pct"] = profile["missing_pct"] * 100
profile = profile.sort_values("missing_pct", ascending=False)

print(profile.head(15))
print("\n")

print("Loan data types:")
print(loans.dtypes)
print("\n")

# -----------------------------
# 4. KPI Feasibility Checks
# -----------------------------

# ---- Loan Status / Default Feasibility ----
print("===== LOAN STATUS CHECK =====")
if "loan_status" in loans.columns:
    print(loans["loan_status"].value_counts(dropna=False).head(10))
else:
    print("loan_status column NOT found")
print("\n")

# ---- Risk Proxy Feasibility ----
print("===== RISK PROXY CHECK =====")

if "grade" in loans.columns:
    print("Grade distribution:")
    print(loans["grade"].value_counts())
    print("\n")

if "int_rate" in loans.columns:
    print("Interest rate summary:")
    print(loans["int_rate"].describe())
    print("\n")

# ---- Borrower Segmentation ----
print("===== BORROWER SEGMENT CHECK =====")

if "annual_inc" in loans.columns:
    print("Annual income missing %:")
    print(loans["annual_inc"].isna().mean() * 100)
    print("\n")

if "emp_length" in loans.columns:
    print("Employment length distribution:")
    print(loans["emp_length"].value_counts(dropna=False))
    print("\n")

# -----------------------------
# 5. Complaint Data Feasibility
# -----------------------------
print("===== COMPLAINT DATA CHECK =====")
print("Columns:")
print(complaints.columns)
print("\n")

if "date_received" in complaints.columns:
    print("Complaint date range:")
    print(complaints["date_received"].min(), "to", complaints["date_received"].max())
    print("\n")

if "product" in complaints.columns:
    print("Top complaint products:")
    print(complaints["product"].value_counts().head(10))
    print("\n")

# -----------------------------
# 6. Macroeconomic Data Feasibility
# -----------------------------
def macro_check(df, name):
    print(f"===== {name.upper()} =====")
    print(df.head(3))
    print("Date range:")
    print(df.iloc[:, 0].min(), "to", df.iloc[:, 0].max())
    print("\n")

macro_check(unrate, "Unemployment Rate")
macro_check(cpi, "Inflation (CPI)")
macro_check(rates, "Federal Funds Rate")

# -----------------------------
# 7. Time Alignment Feasibility
# -----------------------------
print("===== TIME ALIGNMENT CHECK =====")

loan_date_cols = [col for col in loans.columns if "date" in col.lower()]
print("Potential loan date columns:", loan_date_cols)

print("\nMacro datasets are monthly and can be aligned to loan origination month.")

print("\n===== DAY 3 EXPLORATION COMPLETE =====")
