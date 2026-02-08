import psycopg2
from psycopg2.extras import execute_values

def get_connection():
    return psycopg2.connect(
        host="postgres",
        database="US_Treasury",
        user="postgres",
        password="mavi123",
        port="5432"
    )

def load_operating_cash_balance(df):

    conn = get_connection()
    cursor = conn.cursor()

    columns = list(df.columns)
    values = [tuple(row) for row in df.to_numpy()]

    insert_query = f"""
        INSERT INTO operating_cash_balance ({','.join(columns)})
        VALUES %s
        ON CONFLICT (record_date, account_type)
        DO UPDATE SET
        close_today_bal = EXCLUDED.close_today_bal,
        open_today_bal = EXCLUDED.open_today_bal,
        open_month_bal = EXCLUDED.open_month_bal,
        open_fiscal_year_bal = EXCLUDED.open_fiscal_year_bal;
    """

    execute_values(cursor, insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} operating_cash_balance rows successfully.")

def load_public_debt_transactions(df):

    conn = get_connection()
    cursor = conn.cursor()

    columns = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False)]

    insert_query = f"""
        INSERT INTO public_debt_transactions ({','.join(columns)})
        VALUES %s
        ON CONFLICT (
            record_date,
            transaction_type,
            security_type,
            src_line_nbr
        )
        DO UPDATE SET
            transaction_today_amt = EXCLUDED.transaction_today_amt,
            transaction_mtd_amt = EXCLUDED.transaction_mtd_amt,
            transaction_fytd_amt = EXCLUDED.transaction_fytd_amt;
    """

    execute_values(cursor, insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} public_debt_transactions rows successfully.")
