import os
from pathlib import Path

# Base directory for the project (assuming config.py is in the project root)
BASE_DIR = Path(__file__).resolve().parent

# Data Lake Paths
DATA_LAKE_DIR = BASE_DIR / "data_lake"
BRONZE_DIR = DATA_LAKE_DIR / "bronze"
SILVER_DIR = DATA_LAKE_DIR / "silver"
ARCHIVE_DIR = DATA_LAKE_DIR / "archive"

# Dimension Files
DIM_GEO_PATH = SILVER_DIR / "dim_geo.parquet"
DIM_CUST_PATH = SILVER_DIR / "Dim_Customer_Geo.parquet"
DIM_PROD_PATH = SILVER_DIR / "Dim_Product.parquet"

# SQL Server Configuration (defaults can be overridden via environment variables)
SQL_SERVER_NAME = os.getenv("SQL_SERVER_NAME", "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "DataCo_Analytics")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# Helper to ensure directories exist
def ensure_directories():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
