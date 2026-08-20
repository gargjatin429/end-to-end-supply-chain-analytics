# End-to-End Pipeline Walkthrough & Code Review (V2)

This document breaks down the entire supply chain analytical pipeline step by step, following the recent architectural refactoring. For each step, it includes an explanation, the full source code used, and a review/critique for potential future improvements.

---

## Step 1: Configuration Management

**Explanation:**
The project now uses a centralized configuration file to manage environment variables, S3 endpoints (for MinIO), and SQL Server connection strings. This removes hardcoded paths and credentials from the pipeline scripts.

**Code (`config.py`):**
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

**Critique & Improvements:**
- **Improvement:** Great step forward moving away from hardcoded strings. Using `os.getenv()` provides flexibility.
- **Nitpick:** The default S3 credentials (`admin`/`password`) in source control could still be risky if accidentally deployed. Using a dedicated library like `pydantic-settings` or `python-dotenv` for config management is often more robust in larger projects.

---

## Step 2: Reusable Data Transformations (DRY Principle)

**Explanation:**
To resolve the massive code duplication between the single-file and batch pipelines, all the core Polars data cleaning and transformation logic was abstracted into a shared module.

**Code (`pipelines/transformations.py`):**
```python
import polars as pl
from config import DIM_GEO_PATH, DIM_CUST_PATH, DIM_PROD_PATH, get_s3_storage_options

def transform_bronze_to_silver(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw Bronze data into curated Silver data.
    """
    storage_options = get_s3_storage_options()

    df = (
        df
        .with_columns(
            pl.format(
                "{}-{}-{}",
                pl.col("order_year"),
                pl.col("order_month"),
                pl.col("order_day")
            )
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("valid_date_check")
        )
        .filter(pl.col("valid_date_check").is_not_null())
    )

    df = df.unique(maintain_order=True)

    df = df.drop([
        "order_dayofweek",
        "valid_date_check",
        "shipping_mode"
    ])

    df = (
        df
        .with_columns([
            (pl.col("order_item_product_price") * pl.col("order_item_quantity"))
            .alias("gross_sales"),

            (
                (pl.col("order_item_product_price") * pl.col("order_item_quantity"))
                * pl.col("order_item_discount_rate")
            ).alias("discount_amount")
        ])
        .with_columns([
            (pl.col("gross_sales") - pl.col("discount_amount"))
            .alias("net_revenue")
        ])
        .with_columns([
            (pl.col("net_revenue") * pl.col("order_item_profit_ratio"))
            .alias("order_profit_amount")
        ])
        .with_columns([
            (pl.col("net_revenue") - pl.col("order_profit_amount"))
            .alias("total_cost")
        ])
    )

    df = (
        df
        .with_columns([
            (pl.col("total_cost") / pl.col("order_item_quantity"))
            .alias("actual_unit_cost"),

            (pl.col("order_profit_amount") < 0)
            .alias("is_profit_bleeder"),

            (pl.col("days_for_shipping_real")
             - pl.col("days_for_shipment_scheduled"))
            .alias("shipping_delta")
        ])
        .with_columns([
            (
                (pl.col("order_item_product_price") - pl.col("actual_unit_cost"))
                / pl.col("actual_unit_cost")
            ).alias("markup_pct"),

            (
                pl.col("discount_amount")
                / (pl.col("order_profit_amount") + pl.col("discount_amount"))
            ).fill_nan(0.0).alias("margin_leakage_pct")
        ])
    )

    df = df.with_columns([
        pl.when(pl.col("shipping_delta") < 0).then(pl.lit("Early"))
          .when(pl.col("shipping_delta") == 0).then(pl.lit("On Time"))
          .otherwise(pl.lit("Late"))
          .alias("delivery_class"),

        pl.when(pl.col("days_for_shipment_scheduled") == 0).then(pl.lit("Same Day"))
          .when(pl.col("days_for_shipment_scheduled") <= 2).then(pl.lit("First Class"))
          .when(pl.col("days_for_shipment_scheduled") == 3).then(pl.lit("Second Class"))
          .otherwise(pl.lit("Standard Class"))
          .alias("shipping_mode_clean"),

        pl.date(
            pl.col("order_year"),
            pl.col("order_month"),
            pl.col("order_day")
        ).dt.strftime("%A").alias("day_name_str"),

        pl.when(
            pl.date(
                pl.col("order_year"),
                pl.col("order_month"),
                pl.col("order_day")
            )
            .dt.strftime("%A")
            .is_in(["Saturday", "Sunday"])
        )
        .then(pl.lit("Weekend"))
        .otherwise(pl.lit("Weekday"))
        .alias("order_day_type"),

        pl.when(pl.col("order_item_product_price") < 60).then(pl.lit("Budget"))
          .when(pl.col("order_item_product_price") <= 250).then(pl.lit("Mainstream"))
          .otherwise(pl.lit("Premium"))
          .alias("price_segment"),

        (
            pl.col("customer_country").str.replace("EE. UU.", "USA")
            + "_"
            + pl.col("customer_state")
            + " -> "
            + pl.col("order_country")
        ).alias("trade_route")
    ])

    df = (
        df
        .with_columns([
            (pl.col("gross_sales")
             / pl.col("gross_sales").sum().over("category_name"))
            .alias("category_share_pct"),

            pl.col("order_state").count().over("order_state")
            .alias("state_order_count"),

            (pl.col("gross_sales")
             / pl.col("gross_sales").sum().over("market"))
            .alias("market_share_pct")
        ])
        .with_columns([
            pl.when(pl.col("state_order_count") > 100).then(pl.lit("Strategic Hub"))
              .when(pl.col("state_order_count") < 10).then(pl.lit("Expansion Zone"))
              .otherwise(pl.lit("Standard Zone"))
              .alias("state_density_class")
        ])
    )

    dim_geo = pl.read_parquet(DIM_GEO_PATH, storage_options=storage_options)
    dim_cust = pl.read_parquet(DIM_CUST_PATH, storage_options=storage_options)
    dim_prod = pl.read_parquet(DIM_PROD_PATH, storage_options=storage_options)

    df = (
        df
        .join(dim_geo,
              on=["order_state", "order_country", "order_region", "market"],
              how="left")
        .drop(["order_state", "order_country", "order_region", "market"])
        .join(dim_cust,
              on=["customer_state", "customer_country"],
              how="left")
        .drop(["customer_state", "customer_country"])
        .join(dim_prod,
              on=["product_name", "category_name", "department_name"],
              how="left")
        .drop(["product_name", "category_name", "department_name"])
    )

    df = df.sort(
        ["order_year", "order_month", "order_day", "order_item_quantity"]
    )

    df = df.rename({col: col.lower() for col in df.columns})

    return df

```

**Critique & Improvements:**
- **Improvement:** Excellent refactor. This makes the code significantly easier to test, maintain, and version control. You can now write unit tests specifically for `transform_bronze_to_silver`.
- **Nitpick:** The function is quite long (170+ lines). In the future, this could be broken down further into smaller, composable functions (e.g., `clean_dates()`, `calculate_financials()`, `join_dimensions()`).

---

## Step 3: Raw Data Cleaning & Data Scaling (Colab / Local Notebooks)

**Explanation:**
The raw Kaggle dataset must be cleaned to normalize text encodings and fix date parsing issues before SDV model training. The cleaned data is then fed into SDV (CTGAN) on Google Colab to generate synthetic rows.

**Code (`data_scaling/Project_CSV_Clean_For_SDV_Colab.ipynb` extracted code):**
```python
## ⚠️ Important Note (Read Before Running Anything)
'''
This notebook exists **only** to clean and normalize the original Kaggle CSV
so it can be safely used for **model training and synthetic data generation**.

Why this matters:
- SDV / CTGAN expects a **clean, stable CSV**
- Broken encodings, bad dates, and messy column names will cause training to fail
- This cleanup is mandatory before feeding data to any model

Scope:
- The output of this notebook is a **model-ready CSV**
- It is not intended for analysis or dashboards
- All blocks are intentionally separate and may be run independently

If you skip this step and jump straight to training, the model will break.
That's not a bug. That's on you.
'''

## Initial Text Normalization & Column Pruning (Raw Kaggle Cleanup)
'''
This block performs aggressive text normalization on the original Kaggle
DataCo Supply Chain dataset to eliminate encoding issues and unusable columns.

Purpose:
- Remove accented and corrupted Unicode characters
- Normalize all text fields to plain ASCII
- Drop unused, sensitive, or redundant columns
- Produce a stable, model-safe CSV for downstream processing

Notes:
- This is a one-time cleanup step for a single raw file
- Blocks in this notebook are intentionally independent
- File paths and names can be adjusted as needed
'''

import pandas as pd
import unicodedata

def clean_text(text):
    """
    Normalize accented or special Unicode characters into plain ASCII.
    Example: 'São Paulo' -> 'Sao Paulo'
    """
    if pd.isna(text):
        return ""

    text = str(text)

    # Decompose Unicode characters (e.g., é -> e + accent)
    # Retain only base characters
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

# ------------------------------------------------------------------------------
# 1. LOAD RAW DATA
# ------------------------------------------------------------------------------
print("Loading raw dataset for text normalization...")

# 'latin-1' is more forgiving for this dataset's encoding issues
df = pd.read_csv(
    'DataCoSupplyChainDataset.csv',
    encoding='latin-1'
)

# ------------------------------------------------------------------------------
# 2. NORMALIZE COLUMN NAMES
# ------------------------------------------------------------------------------
# Strip accidental whitespace from column headers
df.columns = df.columns.str.strip()

# ------------------------------------------------------------------------------
# 3. APPLY TEXT NORMALIZATION ACROSS ALL TEXT COLUMNS
# ------------------------------------------------------------------------------
print("Normalizing special characters across text columns...")

text_cols = df.select_dtypes(include=['object']).columns

for col in text_cols:
    df[col] = df[col].apply(clean_text)

# ------------------------------------------------------------------------------
# 4. DROP UNUSED / SENSITIVE / REDUNDANT COLUMNS
# ------------------------------------------------------------------------------
cols_to_drop = [
    'Customer Email', 'Customer Fname', 'Customer Lname', 'Customer Password',
    'Customer Street', 'Customer Zipcode', 'Order Id', 'Order Item Id',
    'Customer Id', 'Order Customer Id', 'Product Card Id',
    'Order Item Cardprod Id', 'Category Id', 'Department Id',
    'Product Category Id', 'Product Description', 'Product Image',
    'Latitude', 'Longitude', 'Benefit per order', 'Sales per customer',
    'Delivery Status', 'Late_delivery_risk', 'Customer City', 'Order City',
    'Order Item Discount', 'Sales', 'Order Item Total', 'Order Profit Per Order',
    'Order Zipcode', 'Product Price', 'Product Status',
    'shipping date (DateOrders)'
]

# Drop only columns that actually exist
existing_cols = [c for c in cols_to_drop if c in df.columns]
df_clean = df.drop(columns=existing_cols)

print(f"Dropped {len(existing_cols)} columns.")

# ------------------------------------------------------------------------------
# 5. EXPORT CLEANED DATA
# ------------------------------------------------------------------------------
# 'utf-8-sig' ensures Excel correctly interprets UTF-8 encoding
output_name = 'DataCo_Cleaned_Plain_English.csv'
df_clean.to_csv(
    output_name,
    index=False,
    encoding='utf-8-sig'
)

print(f"Cleanup complete. Output saved as: {output_name}")

## Date Parsing & Temporal Repair (Raw Kaggle Fix)
'''
This block repairs corrupted and inconsistently formatted order date fields
from the original Kaggle dataset.

Purpose:
- Parse multiple inconsistent datetime formats
- Extract year, month, and day as atomic columns
- Correct malformed years (e.g., 0018 → 2018)
- Recompute day-of-week after date correction

Notes:
- This is a best-effort cleanup for a single raw file
- Imperfect rows are tolerated and corrected logically
- This step exists solely to stabilize downstream analysis
'''

import polars as pl
import os

# ------------------------------------------------------------------------------
# 1. FILE SETUP
# ------------------------------------------------------------------------------
folder_path = r"D:\Data Lake\very_raw"
file_name = "supply_chain_sample_new_2.csv"
full_path = os.path.join(folder_path, file_name)

df = pl.read_csv(full_path)

# ------------------------------------------------------------------------------
# 2. CLEAN & PARSE DATE STRINGS
# ------------------------------------------------------------------------------
# Normalize separators and spacing before parsing
df = df.with_columns(
    date_str_clean=(
        pl.col("order date (DateOrders)")
        .str.replace_all("/", "-")
        .str.replace_all("  ", " ")
        .str.strip_chars()
    )
)

# Attempt multiple datetime formats (best-effort parsing)
df_final = df.with_columns(
    parsed_date=pl.coalesce(
        pl.col("date_str_clean").str.to_datetime("%m-%d-%Y %H:%M", strict=False),
        pl.col("date_str_clean").str.to_datetime("%m-%d-%Y %I:%M:%S %p", strict=False),
        pl.col("date_str_clean").str.to_datetime("%m-%d-%y %H:%M", strict=False)
    )
).with_columns(
    Order_Year=pl.col("parsed_date").dt.year(),
    Order_Month=pl.col("parsed_date").dt.month(),
    Order_Day=pl.col("parsed_date").dt.day()
)

# ------------------------------------------------------------------------------
# 3. TEMPORAL CORRECTION (YEAR FIX)
# ------------------------------------------------------------------------------
# Fix malformed years (e.g., 18 -> 2018)
df_final = df_final.with_columns(
    Order_Year=pl.when(pl.col("Order_Year") < 1900)
                  .then(pl.col("Order_Year") + 2000)
                  .otherwise(pl.col("Order_Year"))
)

# ------------------------------------------------------------------------------
# 4. RECOMPUTE DAY OF WEEK
# ------------------------------------------------------------------------------
# Recalculate weekday after correcting the year
df_final = df_final.with_columns(
    Order_DayOfWeek=pl.date(
        pl.col("Order_Year"),
        pl.col("Order_Month"),
        pl.col("Order_Day")
    ).dt.weekday()
).drop(["date_str_clean", "parsed_date"])

# ------------------------------------------------------------------------------
# 5. VALIDATION CHECK
# ------------------------------------------------------------------------------
print("Validating year correction...")

ancient_check = df_final.filter(pl.col("Order_Year") < 1900)

if ancient_check.height == 0:
    print("All years successfully corrected.")
    print(
        df_final.select(
            ["order date (DateOrders)", "Order_Year", "Order_DayOfWeek"]
        ).head(10)
    )
else:
    print(f"Found {ancient_check.height} rows with invalid years.")

# ------------------------------------------------------------------------------
# 6. EXPORT CLEANED DATA
# ------------------------------------------------------------------------------
output_filename = "Cleaned_Data_With_Dates.csv"
output_path = os.path.join(folder_path, output_filename)

print(f"Saving cleaned dataset to: {output_path}")

df_final.write_csv(
    output_path,
    separator=","
)

print("Date cleanup completed.")

## Column Name Standardization (Final Schema Hygiene)
'''
This block standardizes column names after all structural and date fixes
have been applied.

Purpose:
- Enforce consistent snake_case naming
- Remove spaces, brackets, and malformed characters
- Prepare columns for downstream SQL, Polars, and BI compatibility

Notes:
- This step is intentionally isolated
- Naming logic is explicit to avoid silent schema drift
- Output is the final, model-ready dataset
'''

import polars as pl
import os

# ------------------------------------------------------------------------------
# COLUMN NAME STANDARDIZATION
# ------------------------------------------------------------------------------
folder_path = r"D:\Data Lake\very_raw"
file_name = "Cleaned_Data_With_Dates.csv"
full_path = os.path.join(folder_path, file_name)

df2 = pl.read_csv(full_path)

new_columns = []

for col in df2.columns:
    # Step 1: Strip leading/trailing whitespace
    clean_name = col.strip()

    # Step 2: Normalize casing
    clean_name = clean_name.lower()

    # Step 3: Replace problematic characters with underscores
    clean_name = (
        clean_name
        .replace(" ", "_")
        .replace("(", "_")
        .replace(")", "_")
        # Extend replacements here if additional symbols appear
    )

    # Step 4: Collapse multiple underscores
    while "__" in clean_name:
        clean_name = clean_name.replace("__", "_")

    # Step 5: Trim underscores from edges
    clean_name = clean_name.strip("_")

    new_columns.append(clean_name)

# Apply cleaned column names
df2.columns = new_columns

# Verify results
print("Column name standardization complete.")
print(df2.columns)

# ------------------------------------------------------------------------------
# EXPORT FINAL CLEAN DATASET
# ------------------------------------------------------------------------------
output_filename = "fixed_columns_final.csv"
output_path = os.path.join(folder_path, output_filename)

df2.write_csv(output_path, separator=",")

print(f"Final dataset saved to: {output_path}")
```

**Code (`data_scaling/Project_Colab_NB.ipynb` extracted code):**
```python
# Synthetic Data Scaling with SDV (CTGAN / WGAN-GP)
'''
This notebook trains an SDV CTGAN (WGAN-GP architecture) model on a cleaned
transaction-level dataset to generate synthetic data for analytical stress-testing.

Execution Environment:
- Google Colab (Free Tier)
- GPU-enabled runtime (CUDA preferred)

Prerequisites (run before anything else):
1. Install required libraries:
   `!pip install sdv`
2. Mount Google Drive to access input data and save trained models.
   `from google.colab import drive`
   `drive.mount('/content/drive')`

Notes:
- This notebook is not a production ML pipeline.
- The model is used strictly for synthetic data generation, not prediction.
- Personal identifiers were intentionally excluded to preserve stability and focus on aggregate patterns.
'''

### Important
'''
Additional Usage Note:
- File paths and filenames can be changed freely.
  Ensure naming and path consistency is preserved across training,
  generation, and downstream processing.
'''

# Install SDV (required for Colab)
!pip install sdv

# Mount Google Drive
from google.colab import drive
drive.mount('/content/drive')

import torch
import pandas as pd
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import gc

# ------------------------------------------------------------------------------
# 1. HARDWARE CHECK
# ------------------------------------------------------------------------------
# Prefer GPU execution when available (WGAN-GP benefits significantly from CUDA)
device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f"Hardware detected: training will run on {device.upper()}")

# Free memory before training
gc.collect()
torch.cuda.empty_cache()

# ------------------------------------------------------------------------------
# 2. LOAD DATA
# ------------------------------------------------------------------------------
print("Loading cleaned transaction data...")
df_trans = pd.read_csv(
    '/content/drive/MyDrive/DataCo_Synthetic/fixed_columns_final.csv'
)

# ------------------------------------------------------------------------------
# 3. METADATA DEFINITION
# ------------------------------------------------------------------------------
# Automatically infer column types from the dataframe
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df_trans)

# Manual overrides to preserve analytical meaning and stability
# - Prevent SDV from treating state fields as PII
# - Enforce date components as categorical to avoid fractional values
metadata.update_column(column_name='customer_state', sdtype='categorical')
metadata.update_column(column_name='order_state', sdtype='categorical')

metadata.update_column(column_name='order_year', sdtype='categorical')
metadata.update_column(column_name='order_month', sdtype='categorical')
metadata.update_column(column_name='order_day', sdtype='categorical')

print("Final metadata configuration:")
print(metadata.to_dict())

# ------------------------------------------------------------------------------
# 4. CONFIGURE CTGAN (WGAN-GP)
# ------------------------------------------------------------------------------
print("Initializing CTGAN (WGAN-GP architecture)...")

synthesizer = CTGANSynthesizer(
    metadata,
    epochs=500,       # Higher epochs for improved convergence
    batch_size=500,   # Smaller batches for more stable gradient updates
    verbose=True,
    cuda=True         # Force GPU usage when available
)

# ------------------------------------------------------------------------------
# 5. TRAIN MODEL
# ------------------------------------------------------------------------------
print("Starting CTGAN training...")
synthesizer.fit(df_trans)

# ------------------------------------------------------------------------------
# 6. SAVE TRAINED MODEL
# ------------------------------------------------------------------------------
save_path = '/content/drive/MyDrive/DataCo_Synthetic/CTGAN_WGAN_ModelEpochs500.pkl'
synthesizer.save(save_path)

print(f"Model saved to: {save_path}")


## Synthetic Data Quality Evaluation (SDV)
'''
This section evaluates the quality of the generated synthetic dataset against
the original data using SDV's built-in diagnostics and statistical similarity checks.

Purpose:
- Validate that synthetic data preserves structural and statistical properties
- Ensure relationships are usable for analytical stress-testing
- Confirm no major distributional or constraint violations

Notes:
- Evaluation is performed on samples for efficiency
- Scores are used as sanity checks, not as ML benchmarks
- High scores indicate analytical usability, not predictive accuracy
'''

import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import evaluate_quality, run_diagnostic

# ------------------------------------------------------------------------------
# 1. LOAD REAL AND SYNTHETIC DATA (SAMPLED)
# ------------------------------------------------------------------------------
print("Loading data for quality evaluation...")

# Real data sample (sufficient for statistical comparison)
real_df = pd.read_csv(
    '/content/drive/MyDrive/DataCo_Synthetic/fixed_columns_final.csv',
    nrows=150_000
)

# Synthetic data sample generated by CTGAN
fake_df = pd.read_csv(
    '/content/drive/MyDrive/DataCo_Synthetic/DataCo_Synthetic_2M_Safe.csv',
    nrows=1_500_000
)

# ------------------------------------------------------------------------------
# 2. METADATA SETUP
# ------------------------------------------------------------------------------
# Metadata is inferred from real data to define valid structure and constraints
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(real_df)

# ------------------------------------------------------------------------------
# 3. DIAGNOSTIC CHECK
# ------------------------------------------------------------------------------
# Verifies whether the synthetic data violates basic rules or constraints
print("\nRunning diagnostic checks...")
diagnostic = run_diagnostic(
    real_data=real_df,
    synthetic_data=fake_df,
    metadata=metadata
)

# ------------------------------------------------------------------------------
# 4. STATISTICAL QUALITY EVALUATION
# ------------------------------------------------------------------------------
# Measures how closely synthetic distributions match the real data
print("\nRunning statistical quality evaluation...")
report = evaluate_quality(
    real_data=real_df,
    synthetic_data=fake_df,
    metadata=metadata
)

# ------------------------------------------------------------------------------
# 5. RESULTS SUMMARY
# ------------------------------------------------------------------------------
print("\n" + "=" * 40)
print(f"Synthetic Data Quality Score: {report.get_score() * 100:.2f}%")
print("=" * 40)

print("\nDetailed Metric Breakdown:")
print(report.get_properties())

## Quick Synthetic Sample Validation
'''
This section performs a lightweight quality check on a small synthetic sample
generated directly from the trained CTGAN model.

Purpose:
- Sanity-check the trained model before large-scale generation
- Verify that basic statistical structure is preserved
- Catch obvious metadata or training issues early

Notes:
- This is a fast validation step, not a full benchmark
- Results are indicative, not final
'''

# ------------------------------------------------------------------------------
# QUICK SYNTHETIC SAMPLE QUALITY CHECK
# ------------------------------------------------------------------------------
from sdv.evaluation.single_table import evaluate_quality, run_diagnostic

# Generate a synthetic sample directly from the trained model
print("Generating synthetic sample for validation...")
sample_500 = synthesizer.sample(num_rows=200_000)

# Run statistical quality evaluation
print("Running quality evaluation on synthetic sample...")
quality_report = evaluate_quality(
    real_data=df_trans,
    synthetic_data=sample_500,
    metadata=metadata
)

# Output overall quality score
print("\nFinal synthetic data quality score:")
print(quality_report.get_score())

# Optional diagnostic check (useful for debugging schema or constraint issues)
# diagnostic_report = run_diagnostic(
#     real_data=df_trans,
#     synthetic_data=sample_500,
#     metadata=metadata
# )
# print(diagnostic_report.get_score())

## Large-Scale Synthetic Data Generation with Safety Constraints
'''
This section generates a large synthetic dataset (~2M rows) from the trained
CTGAN model while enforcing real-world relational constraints.

Purpose:
- Scale the dataset for analytical stress-testing
- Prevent unrealistic or hallucinated combinations
- Preserve valid geography, product hierarchy, and customer mappings

Approach:
- Generate data in chunks to manage memory
- Reapply "truth tables" derived from real data
- Enforce valid combinations post-generation
'''

import pandas as pd
from sdv.single_table import CTGANSynthesizer
import gc

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
MODEL_PATH = '/content/drive/MyDrive/DataCo_Synthetic/DataCo_WGAN_Model_Epochs500.pkl'
ORIGINAL_DATA_PATH = '/content/drive/MyDrive/DataCo_Synthetic/fixed_columns_final.csv'
OUTPUT_FILENAME = '/content/drive/MyDrive/DataCo_Synthetic/DataCo_Final_2M.csv'

NUM_ROWS_TO_GENERATE = 2_000_000
CHUNK_SIZE = 100_000

# ------------------------------------------------------------------------------
# 1. LOAD MODEL AND REFERENCE DATA
# ------------------------------------------------------------------------------
print("Loading trained CTGAN model and reference dataset...")

synthesizer = CTGANSynthesizer.load(MODEL_PATH)
df_real = pd.read_csv(ORIGINAL_DATA_PATH)

# ------------------------------------------------------------------------------
# 2. BUILD REFERENCE CONSTRAINT TABLES ("TRUTH TABLES")
# ------------------------------------------------------------------------------
# These tables are used to re-enforce valid combinations after sampling

print("Building reference constraint tables...")

# Order geography constraints
valid_order_geo = df_real[
    ["order_state", "order_country", "order_region", "market"]
].drop_duplicates()

# Product hierarchy constraints
valid_products = df_real[
    ["product_name", "category_name", "department_name"]
].drop_duplicates()

# Customer geography constraints
valid_customer_geo = df_real[
    ["customer_state", "customer_country"]
].drop_duplicates()

print(f"Valid order geography combinations: {len(valid_order_geo)}")
print(f"Valid customer geography combinations: {len(valid_customer_geo)}")
print(f"Valid product combinations: {len(valid_products)}")

# ------------------------------------------------------------------------------
# 3. SYNTHETIC DATA GENERATION (CHUNKED)
# ------------------------------------------------------------------------------
total_chunks = NUM_ROWS_TO_GENERATE // CHUNK_SIZE
synthetic_batches = []

print(f"Starting synthetic generation of {NUM_ROWS_TO_GENERATE} rows...")

for i in range(total_chunks):
    print(f"Generating batch {i + 1}/{total_chunks}")

    batch = synthesizer.sample(num_rows=CHUNK_SIZE)

    # --------------------------------------------------------------------------
    # APPLY SAFETY CONSTRAINTS
    # --------------------------------------------------------------------------
    # Enforce valid order geography
    batch = batch.drop(columns=["order_country", "order_region", "market"])
    batch = batch.merge(valid_order_geo, on="order_state", how="inner")

    # Enforce valid product hierarchy
    batch = batch.drop(columns=["category_name", "department_name"])
    batch = batch.merge(valid_products, on="product_name", how="inner")

    # Enforce valid customer geography
    batch = batch.drop(columns=["customer_country"])
    batch = batch.merge(valid_customer_geo, on="customer_state", how="inner")

    synthetic_batches.append(batch)

    # Explicit memory cleanup between batches
    del batch
    gc.collect()

# ------------------------------------------------------------------------------
# 4. FINAL ASSEMBLY AND EXPORT
# ------------------------------------------------------------------------------
print("Combining generated batches...")

df_final = pd.concat(synthetic_batches, ignore_index=True)

# Optional: downsample to exact row count for determinism
df_final = df_final.sample(n=NUM_ROWS_TO_GENERATE, random_state=42)

print("Synthetic data generation complete.")
print(f"Final row count: {len(df_final)}")

print(f"Saving output to: {OUTPUT_FILENAME}")
df_final.to_csv(OUTPUT_FILENAME, index=False)

print("Generation process finished.")

```

**Critique & Improvements:**
- **Critique:** The notebooks still remain isolated from the automated pipeline. While acceptable for a one-off modeling task, in a production ML-ops environment, the training and inference steps would be containerized and orchestrated alongside the data pipelines, rather than run manually in Colab.

---

## Step 4: Analytical Database Initialization (SQL)

**Explanation:**
These scripts set up the data warehouse structure (`DataCo_Analytics`), create dimension/fact tables with foreign key constraints, and add a persisted computed column.

**Code (`sql/01_Database_&_Tables_Creation.sql`):**
```sql
/*
Purpose:
    Initialize the analytical database schema for the DataCo Supply Chain case study.

What this script does:
    - Creates the analytics database
    - Defines dimension tables (Geo, Customer Geo, Product)
    - Defines the central fact table with enforced relationships

Execution Notes:
    - Run this script once before loading any data
    - Dimension tables must be created before the fact table
    - This script assumes SQL Server
*/

-- =============================================================================
-- STEP 1: CREATE DATABASE
-- =============================================================================
CREATE DATABASE DataCo_Analytics;
GO

-- =============================================================================
-- STEP 2: SET DATABASE CONTEXT
-- =============================================================================
-- Prevents accidental table creation in system databases
USE DataCo_Analytics;
GO

-- =============================================================================
-- STEP 3: CREATE DIMENSION TABLES
-- =============================================================================
-- Dimensions must exist before the fact table due to foreign key constraints

CREATE TABLE Dim_Geo (
    geo_id INT PRIMARY KEY,
    order_country NVARCHAR(100),
    order_state NVARCHAR(100),
    order_region NVARCHAR(100),
    market NVARCHAR(50)
);

CREATE TABLE Dim_Customer_Geo (
    customer_geo_id INT PRIMARY KEY,
    customer_country NVARCHAR(100),
    customer_state NVARCHAR(100)
);

CREATE TABLE Dim_Product (
    product_key INT PRIMARY KEY,
    product_name NVARCHAR(255),
    category_name NVARCHAR(100),
    department_name NVARCHAR(100)
);

-- =============================================================================
-- STEP 4: CREATE FACT TABLE
-- =============================================================================
-- Central analytical table containing transactional and derived metrics

CREATE TABLE Fact_Sales (
    -- IDENTIFIERS & KEYS
    order_id INT IDENTITY(10000001, 1)
        PRIMARY KEY CLUSTERED,

    geo_id INT
        FOREIGN KEY REFERENCES Dim_Geo(geo_id),

    customer_geo_id INT
        FOREIGN KEY REFERENCES Dim_Customer_Geo(customer_geo_id),

    product_key INT
        FOREIGN KEY REFERENCES Dim_Product(product_key),

    -- TIME DIMENSIONS
    order_year INT,
    order_month INT,
    order_day INT,
    day_name_str NVARCHAR(20),
    order_day_type NVARCHAR(20),

    -- LOGISTICS & OPERATIONS
    type NVARCHAR(50),
    days_for_shipping_real INT,
    days_for_shipment_scheduled INT,
    shipping_delta INT,
    delivery_class NVARCHAR(50),
    shipping_mode_clean NVARCHAR(50),
    order_status NVARCHAR(50),
    customer_segment NVARCHAR(50),

    -- FINANCIAL METRICS
    order_item_quantity INT,
    order_item_product_price DECIMAL(18, 4),
    order_item_discount_rate DECIMAL(18, 4),
    order_item_profit_ratio DECIMAL(18, 4),
    gross_sales DECIMAL(18, 4),
    discount_amount DECIMAL(18, 4),
    net_revenue DECIMAL(18, 4),
    order_profit_amount DECIMAL(18, 4),
    total_cost DECIMAL(18, 4),
    actual_unit_cost DECIMAL(18, 4),

    -- RISK & STRATEGY FLAGS
    is_profit_bleeder BIT,
    markup_pct DECIMAL(18, 4),
    margin_leakage_pct DECIMAL(18, 4),
    price_segment NVARCHAR(50),
    trade_route NVARCHAR(255),

    -- ANALYTICAL SUPPORT METRICS
    state_order_count INT,
    state_density_class NVARCHAR(50),

    -- AUDIT
    load_timestamp DATETIME DEFAULT GETDATE()
);
GO
```

**Code (`sql/02_add_persisted_columns.sql`):**
```sql
/*
Purpose:
    Add a derived, persisted date column to the Fact_Sales table.

Why this is separate:
    - The base table stores atomic date components (year, month, day)
    - This column reconstructs a full DATE for analytical convenience
    - Keeping it separate avoids coupling schema creation with enrichment

Notes:
    - This column is computed and PERSISTED
    - SQL Server will store the value physically
    - Improves query simplicity and BI compatibility
*/

ALTER TABLE Fact_Sales
ADD order_date AS DATEFROMPARTS(order_year, order_month, order_day) PERSISTED;
GO

```

**Critique & Improvements:**
- **Critique:** The SQL schemas still exist in the default `dbo` namespace, and the indexing strategy (only a PK clustered index) remains unoptimized for heavy analytical query patterns. Consider adding columnstore indexes for the Fact table since this is analytical data.

---

## Step 5: Bronze to Silver Processing (Pipelines)

**Explanation:**
These scripts orchestrate the ingestion of raw data from the Bronze layer (now reading from S3/MinIO), apply the shared transformation logic, and write Parquet files to the Silver layer. It includes proper logging and archival steps.

**Code (`pipelines/Project_Batch_Process.py`):**
```python
import polars as pl
import s3fs
import os
import logging
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_DIR, SILVER_DIR, ARCHIVE_DIR, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
from pipelines.transformations import transform_bronze_to_silver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    storage_options = get_s3_storage_options()

    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    # PHASE 1: DISCOVERY
    # Remove the s3:// prefix for s3fs globbing
    bronze_path_no_scheme = BRONZE_DIR.replace("s3://", "")
    csv_files = fs.glob(f"{bronze_path_no_scheme}/*.csv")

    logging.info(f"Found {len(csv_files)} files to process in {BRONZE_DIR}.")

    # PHASE 2: BATCH PROCESSING
    for i, file_path in enumerate(csv_files, start=1):
        file_name = os.path.basename(file_path)
        full_s3_path = f"s3://{file_path}"
        logging.info(f"Processing file {i}/{len(csv_files)}: {file_name}")

        try:
            # STEP 1: LOAD
            with fs.open(full_s3_path, 'rb') as f:
                df = pl.read_csv(f, encoding="cp1252")

            # STEP 2-7: TRANSFORMATIONS
            df_silver = transform_bronze_to_silver(df)

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

    logging.info("Batch processing complete.")

if __name__ == "__main__":
    main()
```

**Code (`pipelines/Project_Single_File.py`):**
```python
import polars as pl
import s3fs
import os
import logging
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_DIR, SILVER_DIR, ARCHIVE_DIR, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
from pipelines.transformations import transform_bronze_to_silver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    storage_options = get_s3_storage_options()

    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    source_file_name = "DataCo_Final_2M.csv"
    source_file_path = f"{BRONZE_DIR}/{source_file_name}"

    target_file_name = "DataCo_Silver.parquet"
    target_file_path = f"{SILVER_DIR}/{target_file_name}"

    if not fs.exists(source_file_path.replace("s3://", "")):
        logging.error(f"Source file not found: {source_file_path}")
        return

    logging.info(f"Starting single-file pipeline for: {source_file_name}")

    try:
        # STEP 1: LOAD
        with fs.open(source_file_path, 'rb') as f:
            df = pl.read_csv(f, encoding="cp1252")

        logging.info(f"Original row count: {df.height}")

        # STEP 2-7: TRANSFORMATIONS
        df_silver = transform_bronze_to_silver(df)

        # STEP 7: WRITE
        with fs.open(target_file_path, 'wb') as f:
            df_silver.write_parquet(f)

        logging.info(f"Processed file saved to: {target_file_path}")
        logging.info(f"Final row count: {df_silver.height}")

        # STEP 8: ARCHIVAL
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(source_file_name)
        archive_name = f"{name}_{timestamp}{ext}"
        archive_path = f"{ARCHIVE_DIR}/{archive_name}"

        # Move file in S3
        fs.copy(source_file_path.replace("s3://", ""), archive_path.replace("s3://", ""))
        fs.rm(source_file_path.replace("s3://", ""))

        logging.info(f"Archived source file as: {archive_path}")

    except Exception as e:
        logging.error(f"Pipeline failed. Error details: {e}", exc_info=True)

if __name__ == "__main__":
    main()
```

**Critique & Improvements:**
- **Improvement:** The integration of `s3fs` for object storage and the `logging` module drastically improves the maturity of these scripts. They now behave like real cloud-native applications.
- **Nitpick:** There's still a slight repetition between how the batch and single file scripts handle the file system and logging setups.

---

## Step 6: Silver to SQL Loading Pipeline

**Explanation:**
Reads the curated Parquet files from S3/MinIO and loads them into the SQL Server data warehouse using SQLAlchemy. It features a test mode that gracefully falls back to a local SQLite database for local execution.

**Code (`pipelines/Project_Silver_To_SQL.py`):**
```python
import polars as pl
import s3fs
import os
import logging
from sqlalchemy import create_engine
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SILVER_DIR, ARCHIVE_DIR, SQL_SERVER_NAME, SQL_DATABASE, SQL_DRIVER, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLE_NAME = "Fact_Sales"

STRICT_COLUMNS = [
    "geo_id", "customer_geo_id", "product_key",
    "order_year", "order_month", "order_day",
    "day_name_str", "order_day_type",
    "type", "days_for_shipping_real", "days_for_shipment_scheduled",
    "shipping_delta", "delivery_class", "shipping_mode_clean",
    "order_status", "customer_segment",
    "order_item_quantity", "order_item_product_price",
    "order_item_discount_rate", "order_item_profit_ratio",
    "gross_sales", "discount_amount", "net_revenue",
    "order_profit_amount", "total_cost", "actual_unit_cost",
    "is_profit_bleeder", "markup_pct", "margin_leakage_pct",
    "price_segment", "trade_route",
    "state_order_count", "state_density_class"
]

def main():
    logging.info("Starting Silver → SQL fact load pipeline.")

    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    is_testing = os.getenv("TEST_MODE", "false").lower() == "true"

    if is_testing:
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_analytics.db")
        connection_string = f"sqlite:///{db_path}"
        logging.info(f"Running in test mode. Using SQLite at {db_path}")
    else:
        connection_string = (
            f"mssql+pyodbc://@{SQL_SERVER_NAME}/{SQL_DATABASE}"
            f"?driver={SQL_DRIVER}&trusted_connection=yes"
        )

    try:
        if is_testing:
            engine = create_engine(connection_string)
        else:
            engine = create_engine(connection_string, fast_executemany=True)

        with engine.connect():
            pass
        logging.info("Connected to database.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        return

    silver_path_no_scheme = SILVER_DIR.replace("s3://", "")
    parquet_files = fs.glob(f"{silver_path_no_scheme}/Fact_*.parquet")

    single_file = f"{silver_path_no_scheme}/DataCo_Silver.parquet"
    if fs.exists(single_file) and single_file not in parquet_files:
        parquet_files.append(single_file)

    if not parquet_files:
        logging.info("No fact Parquet files found to load.")
        return

    logging.info(f"Found {len(parquet_files)} files to load.")

    for i, file_path in enumerate(parquet_files, start=1):
        file_name = os.path.basename(file_path)
        full_s3_path = f"s3://{file_path}"
        logging.info(f"Processing file {i}/{len(parquet_files)}: {file_name}")

        try:
            with fs.open(full_s3_path, 'rb') as f:
                df = pl.read_parquet(f)

            available_columns = [col for col in STRICT_COLUMNS if col in df.columns]
            if len(available_columns) < len(STRICT_COLUMNS):
                missing = set(STRICT_COLUMNS) - set(available_columns)
                logging.warning(f"File missing strict columns: {missing}")

            df_clean = df.select(available_columns)
            logging.info(f"Loading {df_clean.height} rows into SQL.")

            df_clean.to_pandas().to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            logging.info("Load successful.")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"LOADED_{file_name}_{timestamp}.parquet"
            archive_path = f"{ARCHIVE_DIR}/{archive_name}"

            fs.copy(file_path, archive_path.replace("s3://", ""))
            fs.rm(file_path)
            logging.info(f"Archived file as: {archive_path}")

        except Exception as e:
            logging.error(f"Error loading {file_name}: {e}", exc_info=True)
            logging.info("Skipping file.")

    logging.info("Silver → SQL pipeline completed.")

if __name__ == "__main__":
    main()
```

**Code (`pipelines/Project_Dimension_Table_To_SQL.py`):**
```python
import polars as pl
import s3fs
from sqlalchemy import create_engine
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DIM_GEO_PATH, DIM_CUST_PATH, DIM_PROD_PATH, SQL_SERVER_NAME, SQL_DATABASE, SQL_DRIVER, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DIM_PATHS = {
    "Dim_Geo": DIM_GEO_PATH,
    "Dim_Customer_Geo": DIM_CUST_PATH,
    "Dim_Product": DIM_PROD_PATH
}

def main():
    logging.info("Starting dimension load pipeline.")

    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    is_testing = os.getenv("TEST_MODE", "false").lower() == "true"

    if is_testing:
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_analytics.db")
        connection_string = f"sqlite:///{db_path}"
        logging.info(f"Running in test mode. Using SQLite at {db_path}")
    else:
        connection_string = (
            f"mssql+pyodbc://@{SQL_SERVER_NAME}/{SQL_DATABASE}"
            f"?driver={SQL_DRIVER}&trusted_connection=yes"
        )

    try:
        if is_testing:
            engine = create_engine(connection_string)
        else:
            engine = create_engine(connection_string, fast_executemany=True)

        with engine.connect():
            pass
        logging.info("Connected to database.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        return

    for table_name, file_path in DIM_PATHS.items():
        logging.info(f"Loading dimension table: {table_name}")

        if not fs.exists(file_path.replace("s3://", "")):
            logging.error(f"File not found: {file_path}")
            continue

        try:
            with fs.open(file_path, 'rb') as f:
                df = pl.read_parquet(f)

            logging.info(f"Read {df.height} rows.")

            df.to_pandas().to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            logging.info(f"Loaded {table_name} successfully.")

        except Exception as e:
            logging.error(f"Error loading {table_name}: {e}", exc_info=True)
            logging.info("Skipping this dimension.")

    logging.info("Dimension loading complete.")

if __name__ == "__main__":
    main()
```

**Critique & Improvements:**
- **Improvement:** Adding `fast_executemany=True` to the SQLAlchemy engine solves the previous Pandas bottleneck for bulk inserts into SQL Server. The `TEST_MODE` fallback is a great developer experience feature.
- **Critique:** The script still uses `if_exists="append"` rather than a true UPSERT/MERGE logic. If a file is re-processed, it will duplicate data in the warehouse.
