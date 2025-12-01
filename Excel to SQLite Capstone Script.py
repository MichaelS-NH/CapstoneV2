import pandas as pd
import sqlite3
import os
# -------------------------------------------------------------------------
# URL for the data source (not used programmatically)
# Provided for reference, consider using it for web scraping in the future.
# -------------------------------------------------------------------------
# https://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp?20=E

# -------------------------------------------------------------------------
# Configuration Section
# These variables define input file location, SQLite database path, and 
# the destination table name. By centralizing them here, it makes the 
# script easier to maintain and update.
# -------------------------------------------------------------------------
excel_path = r"Data\Raw Input\ot_delaycause1_DL (1)\Airline_Delay_Cause.csv"
sqlite_path = r"Data\On_Time_Performance.db"
table_name = "On_Time_Performance"
# -------------------------------------------------------------------------
# Future enhancements:
# - Bonus: Add web-scraping logic to automatically fetch the BTS data.
# - Enhancement: Scan the entire "Raw Input" folder and automatically 
#                select the most recent CSV instead of using a fixed path.
# - Enhancement: Standardize or rename input files to avoid inconsistent 
#                naming between months.
# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# STEP 0: Check if input file exists before attempting any operations.
# This prevents the script from crashing with a FileNotFoundError later.
# -------------------------------------------------------------------------

if not os.path.exists(excel_path):
    print("File not found! Check the file name or path:", excel_path)
else:
    print("File found")

# -------------------------------------------------------------------------
# STEP 1: Load CSV into pandas DataFrame.
# Using read_csv assumes the file is a CSV — despite the variable name 
# 'excel_path' implying Excel, this is because I have the CSV open in Excel.
#
# Pandas will infer data types automatically, but BTS datasets 
# include mixed types in numeric columns, so you may see dtype warnings.
#
# Printing row/column counts provides immediate confirmation of successful
# load and file size consistency month to month.
# -------------------------------------------------------------------------
print("Reading Excel file...")
df = pd.read_csv(excel_path)

print(f"Loaded {len(df):,} rows and {len(df.columns)} columns")

# -------------------------------------------------------------------------
# STEP 2: Clean column names
# 
# Strips whitespace, converts to lowercase, replaces spaces with underscores,
# and removes special characters. This is essential because SQLite column 
# names are more reliable when they follow simple conventions.
#
# Example:
#   "Carrier Delay (%)" → "carrier_delay_"
#
# Note: Removing all non-alphanumeric characters can produce duplicate names 
# if two columns differ only by punctuation. You may want to check for that.
# -------------------------------------------------------------------------
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace(' ', '_')
    .str.replace('[^0-9a-zA-Z_]', '', regex=True)
)

# -------------------------------------------------------------------------
# STEP 3: Connect to SQLite database
# 
# sqlite3.connect() will create the DB file automatically if it does not exist.
#
# SQLite is a great choice for Power BI ODBC ingestion because it's lightweight,
# file-based, and easy to rebuild.
# -------------------------------------------------------------------------
conn = sqlite3.connect(sqlite_path)
print("Connected to SQLite database")

# -------------------------------------------------------------------------
# STEP 4: Write DataFrame to SQLite
#
# 'if_exists="replace"' drops any existing table with the same name and 
# recreates it. This ensures a clean overwrite but means you lose historical 
# records. If you want to build a historical dataset, switch to 'append'.
#
# index=False prevents pandas from writing a numeric index column.
# -------------------------------------------------------------------------
df.to_sql(table_name, conn, if_exists='replace', index=False)
print(f"Data written to table: {table_name}")

# -------------------------------------------------------------------------
# STEP 5: Verify row count in SQLite
#
# A simple integrity check — confirms that SQLite contains the same number 
# of rows as the DataFrame. If counts differ, you can catch issues early.
#
# fetchone()[0] extracts the integer from the returned tuple.
# -------------------------------------------------------------------------
count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
print(f"SQLite table '{table_name}' has {count:,} rows")

# -------------------------------------------------------------------------
# Close the SQLite connection cleanly.
# Failing to close can leave write locks on the DB file.
# -------------------------------------------------------------------------

conn.close()
print("Done")
