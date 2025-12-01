import pandas as pd
import sqlite3
import os

## URL for Data: https://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp?20=E

## Configuration - this tells the code where to find data from excel, then where to store it.

excel_path = r"Data\Raw Input\ot_delaycause1_DL (1)\Airline_Delay_Cause.csv"
sqlite_path = r"Data\On_Time_Performance.db"
table_name = "On_Time_Performance"

## Bonus points to web scrape

##new step needed to scan entire 'raw input' folder

# new step to rename file

## STEP 0: Check script can read the file:

import os

if not os.path.exists(excel_path):
    print("File not found! Check the file name or path:", excel_path)
else:
    print("File found")

# === STEP 1: Load Excel ===
print("Reading Excel file...")
df = pd.read_csv(excel_path)

print(f"Loaded {len(df):,} rows and {len(df.columns)} columns")

# === STEP 2: Clean column names (optional, makes SQL easier) ===
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace(' ', '_')
    .str.replace('[^0-9a-zA-Z_]', '', regex=True)
)

# === STEP 3: Connect to SQLite ===
conn = sqlite3.connect(sqlite_path)
print("Connected to SQLite database")

# === STEP 4: Write to database ===
df.to_sql(table_name, conn, if_exists='replace', index=False)
print(f"Data written to table: {table_name}")

# === STEP 5: Verify row count ===
count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
print(f"SQLite table '{table_name}' has {count:,} rows")

conn.close()
print("Done")
