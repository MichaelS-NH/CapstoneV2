import os
import sqlite3
import pandas as pd
import pytest

from capstone_v2_etl_pipeline import (
    file_exists,
    load_csv,
    clean_column_names,
    write_to_sqlite,
    verify_row_count,
)


# -------------------------------------------------------------------------
# Test file existence helper
# -------------------------------------------------------------------------
def test_file_exists(tmp_path):
    test_file = tmp_path / "sample.csv"
    test_file.write_text("a,b,c\n1,2,3")

    assert file_exists(test_file) is True
    assert file_exists(test_file.with_suffix(".missing")) is False


# -------------------------------------------------------------------------
# Test CSV loading
# -------------------------------------------------------------------------
def test_load_csv_success(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b,c\n1,2,3")

    df = load_csv(csv_file)
    assert len(df) == 1
    assert list(df.columns) == ["a", "b", "c"]


def test_load_csv_failure():
    with pytest.raises(FileNotFoundError):
        load_csv("non_existent.csv")


# -------------------------------------------------------------------------
# Test column cleaning
# -------------------------------------------------------------------------
def test_clean_column_names():
    df = pd.DataFrame({"Carrier Delay (%)": [1], "  Flight Number ": [123]})
    cleaned = clean_column_names(df)

    assert list(cleaned.columns) == ["carrier_delay_", "flight_number"]


# -------------------------------------------------------------------------
# Test writing to SQLite
# -------------------------------------------------------------------------
def test_write_to_sqlite_and_row_count(tmp_path):
    # Create a temporary SQLite file
    sqlite_path = tmp_path / "test.db"
    table_name = "TestTable"

    # Create sample dataframe
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    # Write to SQLite
    write_to_sqlite(df, sqlite_path, table_name)

    # Ensure database file was created
    assert os.path.exists(sqlite_path)

    # Verify row count function
    count = verify_row_count(sqlite_path, table_name)
    assert count == 2


# -------------------------------------------------------------------------
# Test verify_row_count on missing table
# -------------------------------------------------------------------------
def test_verify_row_count_missing_table(tmp_path):
    sqlite_path = tmp_path / "new.db"

    # Create an empty DB with no tables
    conn = sqlite3.connect(sqlite_path)
    conn.close()

    with pytest.raises(sqlite3.OperationalError):
        verify_row_count(sqlite_path, "MissingTable")
