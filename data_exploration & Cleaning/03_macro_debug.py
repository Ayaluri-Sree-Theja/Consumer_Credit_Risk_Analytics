import pandas as pd

RAW_DIR = "data/raw"

# -----------------------------
# Load raw macro files
# -----------------------------
cpi = pd.read_csv(f"{RAW_DIR}/CPALTT01USM657N.csv")
dff = pd.read_csv(f"{RAW_DIR}/DFF.csv")
unrate = pd.read_csv(f"{RAW_DIR}/UNRATE.csv")

print("\n==============================")
print(" CPI (CPALTT01USM657N) DEBUG ")
print("==============================")

print("Columns:", cpi.columns.tolist())
print("\nHead:")
print(cpi.head())

print("\nDate range:")
print(cpi["observation_date"].min(), "to", cpi["observation_date"].max())

value_col_cpi = cpi.columns[1]
print("\nValue column name:", value_col_cpi)
print("Non-null %:", round(cpi[value_col_cpi].notna().mean() * 100, 2))

# Check 2018 explicitly
cpi["observation_date"] = pd.to_datetime(cpi["observation_date"], errors="coerce")
cpi_2018 = cpi[(cpi["observation_date"] >= "2018-01-01") &
               (cpi["observation_date"] <= "2018-03-31")]

print("\nRows in 2018 Q1:", len(cpi_2018))
print(cpi_2018)

# -----------------------------
print("\n==============================")
print(" FED FUNDS RATE (DFF) DEBUG ")
print("==============================")

print("Columns:", dff.columns.tolist())
print("\nHead:")
print(dff.head())

print("\nDate range:")
print(dff["observation_date"].min(), "to", dff["observation_date"].max())

value_col_dff = dff.columns[1]
print("\nValue column name:", value_col_dff)
print("Non-null %:", round(dff[value_col_dff].notna().mean() * 100, 2))

dff["observation_date"] = pd.to_datetime(dff["observation_date"], errors="coerce")
dff_2018 = dff[(dff["observation_date"] >= "2018-01-01") &
               (dff["observation_date"] <= "2018-03-31")]

print("\nRows in 2018 Q1:", len(dff_2018))
print(dff_2018.head(10))

# -----------------------------
print("\n==============================")
print(" UNEMPLOYMENT RATE (UNRATE) ")
print("==============================")

print("Columns:", unrate.columns.tolist())
print("\nHead:")
print(unrate.head())

print("\nDate range:")
print(unrate["observation_date"].min(), "to", unrate["observation_date"].max())

value_col_un = unrate.columns[1]
print("\nValue column name:", value_col_un)
print("Non-null %:", round(unrate[value_col_un].notna().mean() * 100, 2))

unrate["observation_date"] = pd.to_datetime(unrate["observation_date"], errors="coerce")
unrate_2018 = unrate[(unrate["observation_date"] >= "2018-01-01") &
                     (unrate["observation_date"] <= "2018-03-31")]

print("\nRows in 2018 Q1:", len(unrate_2018))
print(unrate_2018)

print("\nMacro debug complete.")
