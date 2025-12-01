import os
import sqlite3
import pandas as pd


# -------------------------------------------------------------------------
# File existence check
# -------------------------------------------------------------------------
def file_exists(path: str) -> bool:
    """Return True if the file exists, otherwise False."""
    return os.path.exists(path)


# -------------------------------------------------------------------------
# Load CSV into DataFrame
# -------------------------------------------------------------------------
def load_csv(path: str) -> pd.DataFrame:
    """
    Loads a CSV file into a pandas DataFrame.
    Raises FileNotFoundError if the file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")

    return pd.read_csv(path)


# -------------------------------------------------------------------------
# Clean column names in a DataFrame
# -------------------------------------------------------------------------
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names by:
    - stripping whitespace
    - converting to lowercase
    - replacing spaces with underscores
    - removing non-alphanumeric characters

    Returns a new DataFrame (does not modify in place).
    """
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )
    return df


# -------------------------------------------------------------------------
# Write a DataFrame to SQLite
# -------------------------------------------------------------------------
def write_to_sqlite(df: pd.DataFrame, sqlite_path: str, table_name: str) -> None:
    """
    Writes the DataFrame to a SQLite database.
    Creates the database file if it does not exist.
    Replaces the table if it already exists.
    """
    conn = sqlite3.connect(sqlite_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()


# -------------------------------------------------------------------------
# Verify row count inside SQLite
# -------------------------------------------------------------------------
def verify_row_count(sqlite_path: str, table_name: str) -> int:
    """
    Returns the number of rows in the specified SQLite table.
    Raises an OperationalError if the table does not exist.
    """
    conn = sqlite3.connect(sqlite_path)
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()
    return count

