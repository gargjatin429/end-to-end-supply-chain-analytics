import os

# S3 Configuration
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")

# S3 Buckets / Paths
BRONZE_DIR = "s3://data-lake/bronze"
SILVER_DIR = "s3://data-lake/silver"
ARCHIVE_DIR = "s3://data-lake/archive"

# Dimension Files
DIM_GEO_PATH = f"{SILVER_DIR}/dim_geo.parquet"
DIM_CUST_PATH = f"{SILVER_DIR}/Dim_Customer_Geo.parquet"
DIM_PROD_PATH = f"{SILVER_DIR}/Dim_Product.parquet"

# SQL Server Configuration (defaults can be overridden via environment variables)
SQL_SERVER_NAME = os.getenv("SQL_SERVER_NAME", "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "DataCo_Analytics")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# Helper for Polars S3 kwargs
def get_s3_storage_options():
    return {
        "endpoint_url": S3_ENDPOINT_URL,
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
    }
