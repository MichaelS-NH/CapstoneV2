# etl_pipeline.py

import os
import sqlite3
import pandas as pd


# -------------------------------------------------------------
# Configuration (you can leave this here or externalize later)
# -------------------------------------------------------------
EXCEL_PATH = r"Data\Raw Input\ot_delaycause1_DL (1)\Airline_Delay_Cause.csv"
SQLITE_PATH = r"Data\On_Time_Performance.db"
TABLE_NAME = "On_Time_Performance"


# -------------------------------------------------------------
# STEP 0: File Existence Check
# -------------------------------------------------------------
def file_exists(path: str) -> bool:
    """Return True if the file exists."""
    return os.path.exists(path)


# -------------------------------------------------------------
# STEP 1: Load CSV into a DataFrame
# -------------------------------------------------------------
def load_csv(path: str) -> pd.DataFrame:
    """Load a CSV into a pandas DataFrame."""
    return pd.read_csv(path)


# -------------------------------------------------------------
# STEP 2: Clean column names
# -------------------------------------------------------------
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names for SQLite compatibility."""
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('[^0-9a-zA-Z_]', '', regex=True)
    )
    return df


# -------------------------------------------------------------
# STEP 3: Connect to SQLite
# -------------------------------------------------------------
def connect_sqlite(path: str) -> sqlite3.Connection:
    """Create and return a SQLite connection."""
    return sqlite3.connect(path)


# -------------------------------------------------------------
# STEP 4: Write DataFrame to SQLite
# -------------------------------------------------------------
def write_to_sqlite(df: pd.DataFrame, conn: sqlite3.Connection, table_name: str):
    """Write DataFrame to SQLite using replace mode."""
    df.to_sql(table_name, conn, if_exists="replace", index=False)


# -------------------------------------------------------------
# STEP 5: Validate row count
# -------------------------------------------------------------
def get_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    """Return number of rows in the SQLite table."""
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]


# -------------------------------------------------------------
# FULL ETL RUNNER
# -------------------------------------------------------------
def run_etl(
    csv_path: str = EXCEL_PATH,
    sqlite_path: str = SQLITE_PATH,
    table_name: str = TABLE_NAME,
):
    """Complete ETL pipeline combining all steps."""

    # Step 0: Validate input path
    if not file_exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")

    # Step 1: Load DataFrame
    df = load_csv(csv_path)

    # Step 2: Clean columns
    df = clean_columns(df)

    # Step 3: Connect to SQLite
    conn = connect_sqlite(sqlite_path)

    try:
        # Step 4: Write to database
        write_to_sqlite(df, conn, table_name)

        # Step 5: Validate
        row_count = get_row_count(conn, table_name)

        # Optional return for testing or reporting
        return {
            "rows_loaded": len(df),
            "rows_in_db": row_count,
            "table_name": table_name,
        }

    finally:
        conn.close()
