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
