# End-to-End Pipeline Walkthrough & Brutal Review

This document breaks down the entire supply chain analytical pipeline step by step. For each step, it includes an explanation, the full source code used, and a brutally honest critique for engineering improvements.

---

## Step 1: Raw Data Cleaning (Local Preparation)

**Explanation:**
Before any synthetic data generation can occur, the raw Kaggle dataset must be cleaned. This step normalizes text encodings, drops unused columns, and fixes date parsing issues to ensure the resulting CSV is stable enough for the SDV model to ingest without crashing.

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

**Brutal Critique & Improvements:**
- **Notebooks are not for ETL:** Using a Jupyter Notebook for mandatory pre-processing is an anti-pattern. This should be a version-controlled Python script (`.py`) that can be executed from a command line or orchestrator.
- **Manual Execution:** The user has to manually open this notebook and run cells. This breaks automation.
- **Hardcoded Paths:** Any file paths in here are likely hardcoded, making it difficult for someone else to run it on their machine.

---

## Step 2: Synthetic Data Scaling (Colab)

**Explanation:**
This step takes the cleaned CSV, uploads it to Google Colab, and uses the Synthetic Data Vault (SDV) CTGAN model to generate roughly 2 million synthetic rows. This allows the local pipeline and SQL warehouse to be stress-tested with volumes that exceed Excel's capacity, while circumventing local GPU limitations.

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

**Brutal Critique & Improvements:**
- **Disconnected Process:** Moving data manually to Colab, running a notebook, downloading the data, and putting it back into the local pipeline completely severs the automated flow.
- **Reproducibility:** There is no specific environment/dependency file (`requirements.txt`) attached to this project, meaning if SDV updates its API, this notebook will break for future users.
- **Security / Data Movement:** Uploading local data to a free-tier cloud service and bringing it back is generally frowned upon in enterprise environments due to data governance and egress issues.

---

## Step 3: Analytical Database Initialization (SQL)

**Explanation:**
The data warehouse needs to be set up before any data is loaded. These scripts create the `DataCo_Analytics` database, the dimension tables (`Dim_Geo`, `Dim_Customer_Geo`, `Dim_Product`), and the central `Fact_Sales` table with foreign key constraints. It also includes an optimization to persist a constructed date column.

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

**Brutal Critique & Improvements:**
- **Lack of Schemas:** Everything is created in the default `dbo` schema. In a real data warehouse, you should separate layers (e.g., `stg` for staging, `core` or `edw` for dimensions and facts).
- **Poor Indexing Strategy:** While the `order_id` primary key has a clustered index, there are no non-clustered indexes on the foreign keys (`geo_id`, `product_key`) or filtering columns (dates, statuses). Analytical queries on a 2-million-row fact table will suffer from massive table scans.
- **Identity on Fact Table:** Using an `IDENTITY` column as a primary key on a fact table populated by external pipelines can lead to mismatched IDs if pipelines are re-run or backfilled, as the order of insertion isn't guaranteed.

---

## Step 4: Bronze to Silver Processing (Batch Pipeline)

**Explanation:**
This script represents the core transformation engine. It reads raw Bronze CSV files, cleans the data, calculates financial and operational metrics (like `is_profit_bleeder`, `markup_pct`), joins pre-cleaned dimension tables to form a star schema, and outputs optimized Parquet files into the Silver layer. It also archives the source files for idempotency.

**Code (`pipelines/Project_Batch_Process.py`):**
```python
"""
Purpose:
    Batch Bronze → Silver processing pipeline for supply chain data.

What this script does:
    - Reads raw CSV files from the Bronze layer
    - Cleans and validates records (dates, duplicates, schema issues)
    - Derives financial, operational, and strategic analytical fields
    - Joins curated dimension tables to form a star-schema-ready fact dataset
    - Writes cleaned Parquet files to the Silver layer
    - Archives processed source files to ensure idempotent re-runs

What this script does NOT do:
    - No model training or synthetic data generation
    - No SQL loading or BI logic
    - No production orchestration or scheduling
"""

import polars as pl
import shutil
import os
import glob
from datetime import datetime

# ==============================================================================
# CONFIGURATION & PATHS
# ==============================================================================
# Data Lake Zones
bronze_folder_path = r"D:\Data Lake\Bronze"
silver_folder_path = r"D:\Data Lake\Silver"
archive_folder_path = r"D:\Data Lake\Archive"

# Dimension Tables (pre-cleaned, static Parquet files)
dim_geo_path = r"D:\Data Lake\Silver\dim_geo.parquet"
dim_cust_path = r"D:\Data Lake\Silver\Dim_Customer_Geo.parquet"
dim_prod_path = r"D:\Data Lake\Silver\Dim_Product.parquet"

# ==============================================================================
# PHASE 1: DISCOVERY
# ==============================================================================
# Identify all CSV files present in the Bronze layer
csv_files = glob.glob(os.path.join(bronze_folder_path, "*.csv"))
print(f"Found {len(csv_files)} files to process.\n")

# ==============================================================================
# PHASE 2: BATCH PROCESSING
# ==============================================================================
for i, file_path in enumerate(csv_files, start=1):

    file_name = os.path.basename(file_path)
    print(f"Processing file {i}/{len(csv_files)}: {file_name}")

    try:
        # ----------------------------------------------------------------------
        # STEP 1: LOAD (Extract)
        # ----------------------------------------------------------------------
        df = pl.read_csv(file_path, encoding="cp1252")

        # ----------------------------------------------------------------------
        # STEP 2: DATA VALIDATION & CLEANUP
        # ----------------------------------------------------------------------
        # Validate dates first to avoid propagating invalid records downstream
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

        # Deduplication while preserving source order
        rows_before = df.height
        df = df.unique(maintain_order=True)
        rows_after = df.height

        if rows_before != rows_after:
            print(f"  Dropped {rows_before - rows_after} duplicate rows.")

        # Remove helper and unused source columns
        df = df.drop([
            "order_dayofweek",
            "valid_date_check",
            "shipping_mode"
        ])

        # ----------------------------------------------------------------------
        # STEP 3: FINANCIAL METRIC DERIVATION
        # ----------------------------------------------------------------------
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

        # ----------------------------------------------------------------------
        # STEP 4: OPERATIONAL & STRATEGIC FEATURES
        # ----------------------------------------------------------------------
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

        # Categorical segmentation for analysis
        df = df.with_columns([
            pl.when(pl.col("shipping_delta") < 0).then("Early")
              .when(pl.col("shipping_delta") == 0).then("On Time")
              .otherwise("Late")
              .alias("delivery_class"),

            pl.when(pl.col("days_for_shipment_scheduled") == 0).then("Same Day")
              .when(pl.col("days_for_shipment_scheduled") <= 2).then("First Class")
              .when(pl.col("days_for_shipment_scheduled") == 3).then("Second Class")
              .otherwise("Standard Class")
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
            .then("Weekend")
            .otherwise("Weekday")
            .alias("order_day_type"),

            pl.when(pl.col("order_item_product_price") < 60).then("Budget")
              .when(pl.col("order_item_product_price") <= 250).then("Mainstream")
              .otherwise("Premium")
              .alias("price_segment"),

            (
                pl.col("customer_country").str.replace("EE. UU.", "USA")
                + "_"
                + pl.col("customer_state")
                + " -> "
                + pl.col("order_country")
            ).alias("trade_route")
        ])

        # ----------------------------------------------------------------------
        # STEP 5: CONTEXTUAL WINDOW METRICS
        # ----------------------------------------------------------------------
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
                pl.when(pl.col("state_order_count") > 100).then("Strategic Hub")
                  .when(pl.col("state_order_count") < 10).then("Expansion Zone")
                  .otherwise("Standard Zone")
                  .alias("state_density_class")
            ])
        )

        # ----------------------------------------------------------------------
        # STEP 6: STAR SCHEMA ENRICHMENT
        # ----------------------------------------------------------------------
        dim_geo = pl.read_parquet(dim_geo_path)
        dim_cust = pl.read_parquet(dim_cust_path)
        dim_prod = pl.read_parquet(dim_prod_path)

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

        # ----------------------------------------------------------------------
        # STEP 7: FINAL SORT & WRITE
        # ----------------------------------------------------------------------
        # Sorting ensures stable downstream clustered indexing in SQL
        df = df.sort(
            ["order_year", "order_month", "order_day", "order_item_quantity"]
        )

        # Normalize column naming
        df = df.rename({col: col.lower() for col in df.columns})

        output_name = f"Fact_{os.path.splitext(file_name)[0]}.parquet"
        df.write_parquet(os.path.join(silver_folder_path, output_name))
        print(f"  Saved cleaned data: {output_name}")

        # ----------------------------------------------------------------------
        # STEP 8: ARCHIVAL (IDEMPOTENCY)
        # ----------------------------------------------------------------------
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.csv"
        shutil.move(file_path, os.path.join(archive_folder_path, archive_name))

        print(f"  Archived source file: {archive_name}\n")

    except Exception as e:
        print(f"  Error processing {file_name}: {e}")
        print("  Skipping file and continuing batch job.\n")

print("Batch processing complete.")

```

**Brutal Critique & Improvements:**
- **"Medallion" on Local C: Drive:** Using hardcoded Windows paths (`D:\Data Lake\Bronze`) is not a real Medallion architecture. It's just moving files between local folders. Use a local S3-compatible object store (like MinIO) or a cloud bucket.
- **No Orchestration:** This script must be run manually. A real pipeline would be scheduled via Airflow, Dagster, or Prefect.
- **Silent Failures:** Catching `Exception as e` and simply printing `Skipping file` without a stack trace is dangerous. If a schema changes or memory runs out, the pipeline quietly skips data, leading to incomplete analytics downstream.
- **Lack of Logging:** `print()` is not a logging framework. Use Python's `logging` module to capture timestamps, log levels, and write to structured files.

---

## Step 5: Bronze to Silver Processing (Single File Pipeline)

**Explanation:**
This script does exactly the same transformations as the Batch Process, but it is hardcoded to process a single, massive CSV file instead of iterating through a folder.

**Code (`pipelines/Project_Single_File.py`):**
```python
"""
Purpose:
    Single-file Bronze → Silver processing pipeline for supply chain data.

What this script does:
    - Processes a single large CSV file from the Bronze layer
    - Validates records, removes duplicates, and cleans schema issues
    - Derives financial, operational, and strategic analytical fields
    - Enriches data by joining curated dimension tables
    - Writes a single Parquet fact file to the Silver layer
    - Archives the processed source file to ensure idempotency

What this script does NOT do:
    - No batch orchestration
    - No model training or synthetic data generation
    - No SQL loading or BI logic
"""

import polars as pl
import shutil
import os
from datetime import datetime

# ==============================================================================
# CONFIGURATION & PATHS
# ==============================================================================
# Source file (Bronze Layer)
SOURCE_FILE_PATH = r"D:\Data Lake\Bronze\DataCo_Final_2M.csv"

# Target output (Silver Layer)
TARGET_FILE_PATH = r"D:\Data Lake\Silver\DataCo_Silver.parquet"

# Archive location for processed source files
ARCHIVE_FOLDER = r"D:\Data Lake\Archive"

# Dimension tables (pre-cleaned, static Parquet files)
DIM_PATHS = {
    "geo":  r"D:\Data Lake\Silver\dim_geo.parquet",
    "cust": r"D:\Data Lake\Silver\Dim_Customer_Geo.parquet",
    "prod": r"D:\Data Lake\Silver\Dim_Product.parquet"
}

# ==============================================================================
# PIPELINE EXECUTION
# ==============================================================================
file_name = os.path.basename(SOURCE_FILE_PATH)
print(f"Starting single-file pipeline for: {file_name}")

try:
    # --------------------------------------------------------------------------
    # STEP 1: LOAD & INITIAL CLEANUP
    # --------------------------------------------------------------------------
    # Using cp1252 encoding to handle Western European character sets correctly
    df = pl.read_csv(SOURCE_FILE_PATH, encoding="cp1252")
    print(f"Original row count: {df.height}")

    # Validate dates early to prevent invalid records from propagating
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

    # Deduplicate while preserving source order
    rows_before = df.height
    df = df.unique(maintain_order=True)
    rows_after = df.height

    if rows_before != rows_after:
        print(f"Removed {rows_before - rows_after} duplicate rows.")

    # Drop helper and unused source columns
    df = df.drop([
        "order_dayofweek",
        "valid_date_check",
        "shipping_mode"
    ])

    # --------------------------------------------------------------------------
    # STEP 2: FINANCIAL METRIC DERIVATION (P&L FOUNDATION)
    # --------------------------------------------------------------------------
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

    # --------------------------------------------------------------------------
    # STEP 3: OPERATIONAL & STRATEGIC FEATURES
    # --------------------------------------------------------------------------
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

    # Categorical segmentation for analysis
    df = df.with_columns([
        pl.when(pl.col("shipping_delta") < 0).then("Early")
          .when(pl.col("shipping_delta") == 0).then("On Time")
          .otherwise("Late")
          .alias("delivery_class"),

        pl.when(pl.col("days_for_shipment_scheduled") == 0).then("Same Day")
          .when(pl.col("days_for_shipment_scheduled") <= 2).then("First Class")
          .when(pl.col("days_for_shipment_scheduled") == 3).then("Second Class")
          .otherwise("Standard Class")
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
        .then("Weekend")
        .otherwise("Weekday")
        .alias("order_day_type"),

        pl.when(pl.col("order_item_product_price") < 60).then("Budget")
          .when(pl.col("order_item_product_price") <= 250).then("Mainstream")
          .otherwise("Premium")
          .alias("price_segment"),

        (
            pl.col("customer_country").str.replace("EE. UU.", "USA")
            + "_"
            + pl.col("customer_state")
            + " -> "
            + pl.col("order_country")
        ).alias("trade_route")
    ])

    # --------------------------------------------------------------------------
    # STEP 4: CONTEXTUAL WINDOW METRICS
    # --------------------------------------------------------------------------
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
            pl.when(pl.col("state_order_count") > 100).then("Strategic Hub")
              .when(pl.col("state_order_count") < 10).then("Expansion Zone")
              .otherwise("Standard Zone")
              .alias("state_density_class")
        ])
    )

    # --------------------------------------------------------------------------
    # STEP 5: STAR SCHEMA ENRICHMENT
    # --------------------------------------------------------------------------
    dim_geo = pl.read_parquet(DIM_PATHS["geo"])
    dim_cust = pl.read_parquet(DIM_PATHS["cust"])
    dim_prod = pl.read_parquet(DIM_PATHS["prod"])

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

    # --------------------------------------------------------------------------
    # STEP 6: FINAL SORT & EXPORT
    # --------------------------------------------------------------------------
    # Sorting ensures stable downstream clustered indexing in SQL
    df = df.sort(
        ["order_year", "order_month", "order_day", "order_item_quantity"]
    )

    # Normalize column naming
    df = df.rename({col: col.lower() for col in df.columns})

    df.write_parquet(TARGET_FILE_PATH)
    print(f"Processed file saved to: {TARGET_FILE_PATH}")
    print(f"Final row count: {df.height}")

    # --------------------------------------------------------------------------
    # STEP 7: ARCHIVAL (IDEMPOTENCY)
    # --------------------------------------------------------------------------
    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name, ext = os.path.splitext(file_name)
    archive_name = f"{name}_{timestamp}{ext}"

    shutil.move(SOURCE_FILE_PATH, os.path.join(ARCHIVE_FOLDER, archive_name))
    print(f"Archived source file as: {archive_name}")

except Exception as e:
    print("Pipeline failed.")
    print(f"Error details: {e}")

```

**Brutal Critique & Improvements:**
- **Extreme Code Duplication (Violation of DRY):** This file is almost a 1:1 copy-paste of the transformation logic from `Project_Batch_Process.py`. You should NEVER copy-paste 200 lines of transformation logic. The Polars transformations should be abstracted into a function `def transform_to_silver(df)` located in a shared module that both scripts import.
- **Hardcoded Configuration:** The file paths are completely hardcoded. Use `.env` or a configuration file (like `config.yml`) so the code is environment-agnostic.

---

## Step 6: Silver to SQL Loading Pipeline

**Explanation:**
Once the data is refined in the Silver layer (Parquet), this script connects to the SQL Server database and loads the data into the `Fact_Sales` table. It enforces a strict column schema to ensure only the expected columns are appended, and then archives the loaded Parquet files.

**Code (`pipelines/Project_Silver_To_SQL.py`):**
```python
"""
Purpose:
    Load curated Silver-layer fact data into SQL Server.

What this script does:
    - Reads fact-level Parquet files from the Silver layer
    - Enforces a strict column contract aligned with the SQL table schema
    - Appends data into the SQL Server fact table
    - Archives successfully loaded files to ensure idempotent execution

What this script does NOT do:
    - No transformations or business logic
    - No dimensional modeling
    - No table creation or schema changes
"""

import polars as pl
import pandas as pd
import shutil
import os
import glob
from sqlalchemy import create_engine
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SERVER_NAME = "localhost"
DATABASE = "DataCo_Analytics"
DRIVER = "ODBC Driver 17 for SQL Server"

SILVER_FOLDER = r"D:\Data Lake\Silver"
ARCHIVE_FOLDER = r"D:\Data Lake\archive_silver"
TABLE_NAME = "Fact_Sales"

connection_string = (
    f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE}"
    f"?driver={DRIVER}&trusted_connection=yes"
)

# Explicit column contract matching the SQL table schema exactly
STRICT_COLUMNS = [
    # Keys
    "geo_id", "customer_geo_id", "product_key",

    # Time (year / month / day only)
    "order_year", "order_month", "order_day",
    "day_name_str", "order_day_type",

    # Logistics
    "type", "days_for_shipping_real", "days_for_shipment_scheduled",
    "shipping_delta", "delivery_class", "shipping_mode_clean",
    "order_status", "customer_segment",

    # Financials
    "order_item_quantity", "order_item_product_price",
    "order_item_discount_rate", "order_item_profit_ratio",
    "gross_sales", "discount_amount", "net_revenue",
    "order_profit_amount", "total_cost", "actual_unit_cost",

    # Metrics
    "is_profit_bleeder", "markup_pct", "margin_leakage_pct",
    "price_segment", "trade_route",
    "state_order_count", "state_density_class"
]

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    print("Starting Silver → SQL fact load pipeline.")

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
    # STEP 2: DISCOVER FACT FILES
    # --------------------------------------------------------------------------
    parquet_files = glob.glob(os.path.join(SILVER_FOLDER, "Fact_*.parquet"))

    if not parquet_files:
        print("No fact Parquet files found to load.")
        return

    print(f"Found {len(parquet_files)} files to load.\n")

    # --------------------------------------------------------------------------
    # STEP 3: LOAD LOOP
    # --------------------------------------------------------------------------
    for i, file_path in enumerate(parquet_files, start=1):
        file_name = os.path.basename(file_path)
        print(f"Processing file {i}/{len(parquet_files)}: {file_name}")

        try:
            # Read Parquet
            df = pl.read_parquet(file_path)

            # Enforce strict schema alignment
            df_clean = df.select(STRICT_COLUMNS)
            print(f"Loading {df_clean.height} rows into SQL.")

            # Append to SQL table
            df_clean.to_pandas().to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            print("Load successful.")

            # Archive processed file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"LOADED_{file_name}_{timestamp}.parquet"
            os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
            shutil.move(file_path, os.path.join(ARCHIVE_FOLDER, archive_name))

            print(f"Archived file as: {archive_name}\n")

        except Exception as e:
            print(f"Error loading {file_name}: {e}")
            print("Skipping file.\n")

    print("Silver → SQL pipeline completed.")

if __name__ == "__main__":
    main()

```

**Brutal Critique & Improvements:**
- **Pandas Bottleneck:** You used the lightning-fast Polars library for all transformations, but here you convert the data to Pandas just to use `to_sql()`. Pandas' `to_sql()` is incredibly slow for bulk operations. For SQL Server, you should use the `bcp` utility (Bulk Copy Program), or use SQLAlchemy with `fast_executemany=True` directly from Polars/Arrow.
- **Hardcoded Credentials:** The `connection_string` with `SERVER_NAME` and `DATABASE` is hardcoded. This is a bad practice. DB connections should be passed via environment variables for security and flexibility.
- **Lack of Upsert Logic (MERGE):** The pipeline uses `if_exists="append"`. If a file accidentally gets placed in the Silver folder twice (or an archive step fails), you will insert duplicate rows. A robust pipeline uses a `MERGE` statement (upsert) to update existing records and insert new ones based on a unique business key.
