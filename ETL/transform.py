import pandas as pd

def transform_operating_cash_balance(df):

    # Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # Convert date column
    df["record_date"] = pd.to_datetime(
        df["record_date"],
        errors="coerce"
    ).dt.normalize()

    # Numeric columns (force correct typing)
    numeric_cols = [
        "close_today_bal",
        "open_today_bal",
        "open_month_bal",
        "open_fiscal_year_bal",
        "src_line_nbr",
        "record_fiscal_year",
        "record_fiscal_quarter",
        "record_calendar_year",
        "record_calendar_quarter",
        "record_calendar_month",
        "record_calendar_day"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean string columns
    string_cols = [
        "account_type",
        "table_nbr",
        "table_nm",
        "sub_table_name"
    ]

    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()

    # Drop invalid dates
    df = df.dropna(subset=["record_date"])

    # Remove duplicates
    df = df.drop_duplicates()

    # Optional: enforce primary key logic
    df = df.sort_values("record_date")

    return df

def transform_public_debt_transactions(df):

    # Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # Convert date column
    df["record_date"] = pd.to_datetime(df["record_date"],errors="coerce").dt.normalize()

    # Numeric amount columns
    amount_cols = [
        "transaction_today_amt",
        "transaction_mtd_amt",
        "transaction_fytd_amt"
    ]

    for col in amount_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integer columns
    int_cols = [
        "src_line_nbr",
        "record_fiscal_year",
        "record_fiscal_quarter",
        "record_calendar_year",
        "record_calendar_quarter",
        "record_calendar_month",
        "record_calendar_day"
    ]

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean string columns
    string_cols = [
        "transaction_type",
        "security_market",
        "security_type",
        "security_type_desc",
        "table_nbr",
        "table_nm"
    ]

    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()

    # Handle sparse column
    df["security_type_desc"] = df["security_type_desc"].replace("nan", None)

    # Drop invalid dates
    df = df.dropna(subset=["record_date"])

    # Remove duplicates
    df = df.drop_duplicates()

    return df
