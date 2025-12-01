# test_etl_pipeline.py

import os
import pandas as pd
import sqlite3
import pytest
from unittest.mock import patch, MagicMock

import etl_pipeline as etl


# -------------------------------------------------------------
# TEST: file_exists()
# -------------------------------------------------------------
def test_file_exists_true(tmp_path):
    # Create a temporary file
    test_file = tmp_path / "dummy.csv"
    test_file.write_text("x")

    assert etl.file_exists(str(test_file)) is True


def test_file_exists_false(tmp_path):
    missing = tmp_path / "missing.csv"
    assert etl.file_exists(str(missing)) is False


# -------------------------------------------------------------
# TEST: load_csv()
# -------------------------------------------------------------
def test_load_csv_reads_file(tmp_path):
    csv = tmp_path / "test.csv"
    csv.write_text("a,b\n1,2")

    df = etl.load_csv(str(csv))

    assert len(df) == 1
    assert list(df.columns) == ["a", "b"]


# -------------------------------------------------------------
# TEST: clean_columns()
# -------------------------------------------------------------
def test_clean_columns():
    df = pd.DataFrame({" Airline Delay (%) ": [1]})

    cleaned = etl.clean_columns(df)

    assert "airline_delay_" in cleaned.columns
    assert cleaned.shape == (1, 1)


# -------------------------------------------------------------
# TEST: connect_sqlite()
# -------------------------------------------------------------
def test_connect_sqlite_creates_db(tmp_path):
    db_path = tmp_path / "test.db"

    conn = etl.connect_sqlite(str(db_path))
    conn.close()

    assert os.path.exists(db_path)


# -------------------------------------------------------------
# TEST: write_to_sqlite() and get_row_count()
# -------------------------------------------------------------
def test_write_and_count(tmp_path):
    # Setup
    db_path = tmp_path / "test.db"
    conn = etl.connect_sqlite(str(db_path))

    df = pd.DataFrame({"col1": [1, 2, 3]})

    # Write to SQLite
    etl.write_to_sqlite(df, conn, "test_table")

    # Check row count
    count = etl.get_row_count(conn, "test_table")

    assert count == 3

    conn.close()


# -------------------------------------------------------------
# TEST: run_etl() end-to-end with mocks
# -------------------------------------------------------------
@patch("etl_pipeline.write_to_sqlite")
@patch("etl_pipeline.connect_sqlite")
@patch("etl_pipeline.clean_columns")
@patch("etl_pipeline.load_csv")
@patch("etl_pipeline.file_exists")
def test_run_etl_pipeline(
    mock_exists,
    mock_load,
    mock_clean,
    mock_connect,
    mock_write,
):
    # Mock behaviors
    mock_exists.return_value = True

    mock_load.return_value = pd.DataFrame({"a": [1, 2]})
    mock_clean.return_value = pd.DataFrame({"a": [1, 2]})

    fake_conn = MagicMock()
    mock_connect.return_value = fake_conn

    # Fake row count
    fake_conn.execute.return_value.fetchone.return_value = [2]

    # Run ETL
    result = etl.run_etl("dummy.csv", "test.db", "table")

    # Assertions
    assert result["rows_loaded"] == 2
    assert result["rows_in_db"] == 2
    assert result["table_name"] == "table"

    # Ensure functions were called
    mock_exists.assert_called_once()
    mock_load.assert_called_once()
    mock_clean.assert_called_once()
    mock_connect.assert_called_once()
    mock_write.assert_called_once()
