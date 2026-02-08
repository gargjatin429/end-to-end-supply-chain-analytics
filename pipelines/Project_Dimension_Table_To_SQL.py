"""
Purpose:
    Load dimension tables from the Silver layer into SQL Server.

What this script does:
    - Reads pre-cleaned dimension Parquet files from the Silver layer
    - Appends dimension data into existing SQL Server tables
    - Establishes the foundational lookup tables for analytical joins

What this script does NOT do:
    - No transformations or business logic
    - No fact table loading
    - No table creation or schema management
"""

import polars as pl
from sqlalchemy import create_engine

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# SQL Server connection details
SERVER_NAME = "localhost"
DATABASE = "DataCo_Analytics"
DRIVER = "ODBC Driver 17 for SQL Server"

# SQLAlchemy connection string (Windows Authentication)
connection_string = (
    f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE}"
    f"?driver={DRIVER}&trusted_connection=yes"
)

# Dimension table sources (Silver Layer)
DIM_PATHS = {
    "Dim_Geo": r"D:\Data Lake\Silver\dim_geo.parquet",
    "Dim_Customer_Geo": r"D:\Data Lake\Silver\Dim_Customer_Geo.parquet",
    "Dim_Product": r"D:\Data Lake\Silver\Dim_Product.parquet"
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    print("Starting dimension load pipeline.")

    # --------------------------------------------------------------------------
    # STEP 1: CONNECT TO SQL SERVER
    # --------------------------------------------------------------------------
    try:
        engine = create_engine(connection_string)
        with engine.connect():
            pass
        print("Connected to SQL Server.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # --------------------------------------------------------------------------
    # STEP 2: LOAD DIMENSION TABLES
    # --------------------------------------------------------------------------
    for table_name, file_path in DIM_PATHS.items():
        print(f"Loading dimension table: {table_name}")

        try:
            # Read Parquet from Silver layer
            df = pl.read_parquet(file_path)
            print(f"Read {df.height} rows.")

            # Append to SQL Server table
            df.to_pandas().to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            print(f"Loaded {table_name} successfully.\n")

        except Exception as e:
            print(f"Error loading {table_name}: {e}")
            print("Skipping this dimension.\n")

    print("Dimension loading complete.")

if __name__ == "__main__":
    main()
