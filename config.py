import os

class ConfigurationError(Exception):
    pass

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

# S3 Configuration
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:9000")

if TEST_MODE:
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")
    SQL_SERVER_NAME = os.getenv("SQL_SERVER_NAME", "localhost")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "DataCo_Analytics")
    SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
else:
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    SQL_SERVER_NAME = os.getenv("SQL_SERVER_NAME")
    SQL_DATABASE = os.getenv("SQL_DATABASE")
    SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    missing_vars = []
    if not S3_ACCESS_KEY: missing_vars.append("S3_ACCESS_KEY")
    if not S3_SECRET_KEY: missing_vars.append("S3_SECRET_KEY")
    if not SQL_SERVER_NAME: missing_vars.append("SQL_SERVER_NAME")
    if not SQL_DATABASE: missing_vars.append("SQL_DATABASE")

    if missing_vars:
        raise ConfigurationError(f"Missing required production environment variables: {', '.join(missing_vars)}")

# S3 Buckets / Paths
BRONZE_DIR = "s3://data-lake/bronze"
SILVER_DIR = "s3://data-lake/silver"
ARCHIVE_DIR = "s3://data-lake/archive"

# Dimension Files
DIM_GEO_PATH = f"{SILVER_DIR}/dim_geo.parquet"
DIM_CUST_PATH = f"{SILVER_DIR}/Dim_Customer_Geo.parquet"
DIM_PROD_PATH = f"{SILVER_DIR}/Dim_Product.parquet"

# Helper for Polars S3 kwargs
def get_s3_storage_options():
    return {
        "endpoint_url": S3_ENDPOINT_URL,
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
    }
