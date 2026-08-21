# Refactored Workflow: End-to-End Medallion Pipeline Implementation

## Assumptions
- The task extracted here represents the holistic pipeline processing logic: from configuring the S3 object store, reading Bronze data, applying transformations via Polars, saving to Silver, and finally bulk-loading to a SQL Server database.
- The code provided is a direct reflection of the current repository state, presented step-by-step to outline the data flow.

## Section 1: Step-by-Step Implementation (with code)

- Step 1: Centralized Configuration Management
  This step establishes the connection strings, credentials, and paths for the MinIO (S3) object store and the SQL Server database. It leverages environment variables with fallbacks to defaults for local development.

```python
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
```

- Step 2: Bronze Data Discovery and Batch Ingestion
  The main batch processor connects to the S3 bucket using `s3fs` and globs all incoming `.csv` files in the Bronze layer. It iterates over the files, opening them directly from the object store into Polars DataFrames.

```python
import polars as pl
import s3fs
import os
import logging
from datetime import datetime

# Assuming config imports are present...

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    # PHASE 1: DISCOVERY
    bronze_path_no_scheme = BRONZE_DIR.replace("s3://", "")
    csv_files = fs.glob(f"{bronze_path_no_scheme}/*.csv")
    logging.info(f"Found {len(csv_files)} files to process in {BRONZE_DIR}.")

    # PHASE 2: BATCH PROCESSING
    for i, file_path in enumerate(csv_files, start=1):
        file_name = os.path.basename(file_path)
        full_s3_path = f"s3://{file_path}"

        try:
            # STEP 1: LOAD
            with fs.open(full_s3_path, 'rb') as f:
                df = pl.read_csv(f, encoding="cp1252")

            # (Transformation logic called here...)
```

- Step 3: Bronze to Silver Data Transformations
  Raw CSV data is cleaned, validated (dates), augmented with computed financial metrics (gross sales, net revenue, margins, shipping deltas), dimensionally mapped via joins to existing Parquet dimension files, and formatted for the Silver layer.

```python
def transform_bronze_to_silver(df: pl.DataFrame) -> pl.DataFrame:
    storage_options = get_s3_storage_options()

    # Date parsing and deduplication
    df = (
        df.with_columns(
            pl.format("{}-{}-{}", pl.col("order_year"), pl.col("order_month"), pl.col("order_day"))
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("valid_date_check")
        )
        .filter(pl.col("valid_date_check").is_not_null())
        .unique(maintain_order=True)
        .drop(["order_dayofweek", "valid_date_check", "shipping_mode"])
    )

    # Financial and Operational Metrics
    df = (
        df.with_columns([
            (pl.col("order_item_product_price") * pl.col("order_item_quantity")).alias("gross_sales"),
            ((pl.col("order_item_product_price") * pl.col("order_item_quantity")) * pl.col("order_item_discount_rate")).alias("discount_amount")
        ])
        .with_columns([(pl.col("gross_sales") - pl.col("discount_amount")).alias("net_revenue")])
        .with_columns([(pl.col("net_revenue") * pl.col("order_item_profit_ratio")).alias("order_profit_amount")])
        .with_columns([(pl.col("net_revenue") - pl.col("order_profit_amount")).alias("total_cost")])
        .with_columns([
            (pl.col("total_cost") / pl.col("order_item_quantity")).alias("actual_unit_cost"),
            (pl.col("order_profit_amount") < 0).alias("is_profit_bleeder"),
            (pl.col("days_for_shipping_real") - pl.col("days_for_shipment_scheduled")).alias("shipping_delta")
        ])
    )

    # Categorization and Window Functions
    df = df.with_columns([
        pl.when(pl.col("shipping_delta") < 0).then(pl.lit("Early"))
          .when(pl.col("shipping_delta") == 0).then(pl.lit("On Time"))
          .otherwise(pl.lit("Late")).alias("delivery_class"),
        # ... additional categorical mappings (shipping mode, price segment, order day type) ...
        (pl.col("gross_sales") / pl.col("gross_sales").sum().over("category_name")).alias("category_share_pct")
    ])

    # Dimensional Joins
    dim_geo = pl.read_parquet(DIM_GEO_PATH, storage_options=storage_options)
    dim_cust = pl.read_parquet(DIM_CUST_PATH, storage_options=storage_options)
    dim_prod = pl.read_parquet(DIM_PROD_PATH, storage_options=storage_options)

    df = (
        df.join(dim_geo, on=["order_state", "order_country", "order_region", "market"], how="left")
          .drop(["order_state", "order_country", "order_region", "market"])
          # ... additional joins for cust and prod ...
    )

    return df.sort(["order_year", "order_month", "order_day", "order_item_quantity"]).rename({col: col.lower() for col in df.columns})
```

- Step 4: Write Curated Data to Silver and Archive Source
  After transformation, the script saves the Polars DataFrame directly back to S3 as a columnar Parquet file. It then achieves idempotency by archiving the original Bronze CSV file to prevent double-processing.

```python
            # STEP 7: WRITE
            output_name = f"Fact_{os.path.splitext(file_name)[0]}.parquet"
            output_path = f"{SILVER_DIR}/{output_name}"

            with fs.open(output_path, 'wb') as f:
                df_silver.write_parquet(f)

            logging.info(f"Saved cleaned data: {output_path}")

            # STEP 8: ARCHIVAL
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.csv"
            archive_path = f"{ARCHIVE_DIR}/{archive_name}"

            # Move file in S3
            fs.copy(file_path, archive_path.replace("s3://", ""))
            fs.rm(file_path)
            logging.info(f"Archived source file: {archive_path}")

        except Exception as e:
            logging.error(f"Error processing {file_name}: {e}", exc_info=True)
            logging.info("Skipping file and continuing batch job.")
```

- Step 5: Silver to SQL Server Serving Pipeline
  A secondary process reads the curated Silver Parquet files. It filters columns based on a strict target schema, determines whether it is in `TEST_MODE` (using SQLite) or production (using MS SQL Server with `fast_executemany`), and performs a chunked bulk load via Pandas to SQLAlchemy. After a successful load, it archives the Parquet file.

```python
# Extract from Project_Silver_To_SQL.py

TABLE_NAME = "Fact_Sales"
STRICT_COLUMNS = [ "geo_id", "customer_geo_id", "product_key", "order_year", "..." ] # Truncated for brevity

def main():
    fs = s3fs.S3FileSystem(key=S3_ACCESS_KEY, secret=S3_SECRET_KEY, client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
    is_testing = os.getenv("TEST_MODE", "false").lower() == "true"

    if is_testing:
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_analytics.db")
        connection_string = f"sqlite:///{db_path}"
        engine = create_engine(connection_string)
    else:
        connection_string = f"mssql+pyodbc://@{SQL_SERVER_NAME}/{SQL_DATABASE}?driver={SQL_DRIVER}&trusted_connection=yes"
        engine = create_engine(connection_string, fast_executemany=True)

    silver_path_no_scheme = SILVER_DIR.replace("s3://", "")
    parquet_files = fs.glob(f"{silver_path_no_scheme}/Fact_*.parquet")

    for i, file_path in enumerate(parquet_files, start=1):
        file_name = os.path.basename(file_path)
        full_s3_path = f"s3://{file_path}"

        try:
            with fs.open(full_s3_path, 'rb') as f:
                df = pl.read_parquet(f)

            available_columns = [col for col in STRICT_COLUMNS if col in df.columns]
            df_clean = df.select(available_columns)

            df_clean.to_pandas().to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = f"{ARCHIVE_DIR}/LOADED_{file_name}_{timestamp}.parquet"
            fs.copy(file_path, archive_path.replace("s3://", ""))
            fs.rm(file_path)

        except Exception as e:
            logging.error(f"Error loading {file_name}: {e}", exc_info=True)

## Detailed Review
1. Overall architecture assessment: The architecture embraces a Medallion pattern (Bronze -> Silver -> Gold/SQL) which is standard and conceptually sound. Using S3 (MinIO) as a data lake decouples storage from compute, which is great. However, it relies heavily on custom Python scripts acting as orchestrators instead of dedicated orchestration tools (Airflow/Dagster), meaning state management, retries, and dependency tracking are manual or absent.
2. Performance considerations: Polars is extremely fast for single-node data transformations and will crunch millions of rows efficiently. However, moving data from S3, transforming in memory, and writing back is bound by network IO. The shift to Pandas right before `to_sql` causes an unnecessary memory overhead, bypassing Polars' native database insertion capabilities (or Arrow database bulk loaders). `fast_executemany` on SQLAlchemy is utilized well for SQL Server.
3. Security implications: Falling back to `"admin"` / `"password"` in `config.py` is a classic security risk, even if "intended" for local development. There is no TLS enforcement for S3 (`http://`). Connecting to SQL Server with `trusted_connection=yes` assumes the executing environment has IAM/Active Directory rights, which is fine locally but breaks down in cross-domain cloud environments without explicit identity management.
4. Maintainability evaluation: Code is relatively DRY with a dedicated `transformations.py`, but it contains enormous, chained `with_columns` blocks. If one transformation fails, debugging which expression threw the error is notoriously difficult in Polars. Hardcoded column names and mappings throughout the transformation logic mean any schema drift in source systems will require immediate code changes.
5. Testability assessment: `TEST_MODE` switching to SQLite is a decent implementation for integration testing the database loader. However, there are no unit tests visible for the individual transformation logic. A massive 80-line Polars transformation function is virtually impossible to unit test effectively without breaking it apart.
6. Dependency analysis: The stack requires `polars`, `s3fs`, `pandas`, `sqlalchemy`, and `pyodbc`. This is a somewhat heavy dependency footprint for simple ingestion. Mixing Polars for transform and Pandas for loading introduces dependency bloat (why not stick to Polars?).

## Flaws & Limitations
1. **Flaw: Loading Dimensions directly inside the Transformation Function**
   - **Why it's a flaw:** `transform_bronze_to_silver` makes network calls to S3 to read `dim_geo.parquet`, `Dim_Customer_Geo.parquet`, and `Dim_Product.parquet` *every single time* the function is called.
   - **Impact:** In a batch loop processing 100 CSVs, it will download those dimension files from S3 100 times, creating massive, unnecessary network latency and compute overhead.
   - **Suggested Fix:** Inject the dimension dataframes as arguments to the function (`def transform(df, dim_geo, dim_cust, ...)`), load them *once* outside the batch loop in `main()`, and pass them in.

2. **Flaw: Archiving Implementation Leaves Partial State (Non-Atomic)**
   - **Why it's a flaw:** In both ingestion scripts, the database load (or S3 write) happens, and *then* the source file is moved/deleted. If the process crashes immediately after writing but before deleting, the file is processed again on the next run.
   - **Impact:** Duplicate data in Silver or SQL Server (unless primary key constraints are strictly enforced in SQL, which isn't shown).
   - **Suggested Fix:** Implement atomic transactions where possible, or use a processed-files manifest (state table) to track ingestion status before deletion.

3. **Flaw: Blindly catching all exceptions in Batch Loop**
   - **Why it's a flaw:** `except Exception as e:` logs the error and immediately skips to the next file, swallowing potentially fatal system errors (like running out of memory or losing database connection).
   - **Impact:** If the database goes offline, the script will loop through every file, fail, "skip", and effectively do nothing while claiming the batch is complete.
   - **Suggested Fix:** Catch specific exceptions (e.g., `pl.ComputeError`, `pl.SchemaError`). If it's a connection or system error, fail fast and abort the pipeline.

4. **Flaw: Using Pandas to push Polars Data to SQL Server**
   - **Why it's a flaw:** Converting a Polars DataFrame to a Pandas DataFrame via `.to_pandas()` forces memory reallocation and strips away Polars' memory efficiencies right at the most critical bottleneck (database IO).
   - **Impact:** The pipeline will consume 2-3x the memory needed right at the end of the script, risking OOM errors on large files.
   - **Suggested Fix:** Use Polars native `df.write_database(connection_string, table_name, engine="adbc")` for significantly faster, zero-copy Arrow database ingestion.

5. **Flaw: Brittle Dimension Joining Logic**
   - **Why it's a flaw:** The joins to `dim_geo`, `dim_cust`, etc. assume a perfect 1:1 match. It uses a `left` join, which is safe, but there is no validation post-join to check if nulls were introduced (e.g., a new product was sold that isn't in the dimension table).
   - **Impact:** Facts will be loaded with NULL dimension keys, completely breaking aggregation in the PowerBI dashboards later.
   - **Suggested Fix:** Add an assertion or logging check after the join: `if df["product_key"].null_count() > 0: raise IncompleteDimensionError(...)`.

6. **Flaw: Unsafe file string replacement in archive logic**
   - **Why it's a flaw:** `archive_path.replace("s3://", "")` and `fs.copy(file_path, ...)` logic is brittle. If `s3://` accidentally appears somewhere else in the path name, it breaks.
   - **Impact:** Files might be archived to incorrect paths or the script crashes on `fs.copy`.
   - **Suggested Fix:** Use Python's `urllib.parse.urlparse` or dedicated Pathlib extensions (like `upath.UPath`) to handle object store URI schemes safely.

## Brutal Review
What am I even looking at? This pipeline is a classic example of "it works on my machine with one file" masquerading as an "enterprise-grade Medallion architecture."

Let’s start with `transformations.py`. You've jammed business logic, financial math, categorical binning, date parsing, AND dimension lookups into a single 100-line monolithic function. If a single column name changes upstream, the entire pipeline detonates, and because you've chained everything into one massive `with_columns` block, the stack trace won't even tell you which specific line failed.

Worse, you are reading dimension tables from S3 *inside* the transformation function. Do you hate network bandwidth? If `Project_Batch_Process.py` iterates over 500 files, you are downloading `Dim_Product.parquet` from S3 500 times. That is rookie-level inefficiency. A senior engineer loads those dimensions *once* into memory before the loop starts and passes them by reference.

Moving on to `Project_Silver_To_SQL.py`. You process data beautifully fast in Polars, and then you slam on the brakes to convert it to Pandas *just* to use `to_sql`. Why? Polars has native ADBC database writers that are orders of magnitude faster. You are wasting memory and CPU cycles because you couldn't be bothered to read the Polars documentation on database connectivity.

Finally, your error handling is abysmal. `except Exception as e:` followed by `logging.info("Skipping file")`. Are you joking? If your SQL Server drops the connection, your script will happily chew through the entire S3 bucket, fail on every single file, log an error nobody will read, and then exit with a success code. Fail fast. If the database is unreachable, kill the process.

**Would I approve this PR?**
**No.** Absolutely not.
**Justification:** While the business logic is sound and the use of Polars is a step in the right direction, the architectural decision to read dimensions in a loop and the reckless catch-all error handling make this code unsafe for any production environment. Fix the dimension injection, implement native Polars SQL writing, and handle exceptions properly. Then we can talk.