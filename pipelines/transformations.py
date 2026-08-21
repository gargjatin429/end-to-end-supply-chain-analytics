import polars as pl
import logging

class DataValidationError(Exception):
    """Exception raised for errors in the incoming data schema or validation."""
    pass

class IncompleteDimensionError(Exception):
    """Exception raised when a join with a dimension table results in NULL keys."""
    pass


def _validate_schema(df: pl.LazyFrame) -> pl.LazyFrame:
    # We inspect schema from lazy frame
    cols = df.collect_schema().names()
    required_columns = [
        "order_year", "order_month", "order_day",
        "order_item_product_price", "order_item_quantity",
        "order_item_discount_rate", "order_item_profit_ratio",
        "days_for_shipping_real", "days_for_shipment_scheduled",
        "customer_country", "customer_state", "order_country", "order_state",
        "order_region", "market", "product_name", "category_name", "department_name"
    ]

    missing_columns = [col for col in required_columns if col not in cols]
    if missing_columns:
        raise DataValidationError(f"Missing required columns in Bronze data: {missing_columns}")

    return df

def _safe_cast_and_filter(df: pl.LazyFrame) -> pl.LazyFrame:
    """Explicitly cast types to prevent schema math errors and filter impossible values."""
    df = df.with_columns([
        pl.col("order_item_product_price").cast(pl.Float64, strict=False),
        pl.col("order_item_quantity").cast(pl.Int64, strict=False),
        pl.col("order_item_discount_rate").cast(pl.Float64, strict=False),
        pl.col("order_item_profit_ratio").cast(pl.Float64, strict=False),
        pl.col("days_for_shipping_real").cast(pl.Int64, strict=False),
        pl.col("days_for_shipment_scheduled").cast(pl.Int64, strict=False),
    ])

    # Filter physical impossibilities
    df = df.filter(
        (pl.col("order_item_product_price") >= 0) &
        (pl.col("order_item_quantity") > 0) & # Quantity > 0 protects division later
        (pl.col("days_for_shipment_scheduled") >= 0)
    )
    return df

def _parse_dates(df: pl.LazyFrame) -> pl.LazyFrame:
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
    df = df.drop(["order_dayofweek", "valid_date_check", "shipping_mode"], strict=False)
    return df

def _calculate_financials(df: pl.LazyFrame) -> pl.LazyFrame:
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
            # Safe division for unit cost
            pl.when(pl.col("order_item_quantity") > 0)
              .then(pl.col("total_cost") / pl.col("order_item_quantity"))
              .otherwise(pl.lit(0.0))
              .alias("actual_unit_cost"),

            (pl.col("order_profit_amount") < 0)
            .alias("is_profit_bleeder"),

            (pl.col("days_for_shipping_real")
             - pl.col("days_for_shipment_scheduled"))
            .alias("shipping_delta")
        ])
        .with_columns([
            # Safe division for markup
            pl.when(pl.col("actual_unit_cost") > 0)
              .then((pl.col("order_item_product_price") - pl.col("actual_unit_cost")) / pl.col("actual_unit_cost"))
              .otherwise(pl.lit(0.0))
              .alias("markup_pct"),

            # Safe division for margin leakage
            pl.when((pl.col("order_profit_amount") + pl.col("discount_amount")) != 0)
              .then(pl.col("discount_amount") / (pl.col("order_profit_amount") + pl.col("discount_amount")))
              .otherwise(pl.lit(0.0))
              .fill_nan(0.0)
              .alias("margin_leakage_pct")
        ])
    )
    return df

def _apply_business_rules(df: pl.LazyFrame) -> pl.LazyFrame:
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
    return df

def _join_dimensions(df: pl.LazyFrame, dim_geo: pl.LazyFrame, dim_cust: pl.LazyFrame, dim_prod: pl.LazyFrame) -> pl.LazyFrame:
    df = (
        df
        .join(dim_geo,
              on=["order_state", "order_country", "order_region", "market"],
              how="left")
        .join(dim_cust,
              on=["customer_state", "customer_country"],
              how="left")
        .join(dim_prod,
              on=["product_name", "category_name", "department_name"],
              how="left")
    )

    # We drop the keys in the lazy frame graph.
    # NULL checking is deferred to the main script after collection because LazyFrames cannot be filtered/counted natively until executed.
    df = df.drop([
        "order_state", "order_country", "order_region", "market",
        "customer_state", "customer_country",
        "product_name", "category_name", "department_name"
    ])

    return df

def transform_bronze_to_silver(
    df: pl.LazyFrame,
    dim_geo: pl.LazyFrame,
    dim_cust: pl.LazyFrame,
    dim_prod: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Transforms raw Bronze data into curated Silver data using Polars Lazy execution graph.
    """
    df = _validate_schema(df)
    df = _safe_cast_and_filter(df)
    df = _parse_dates(df)
    df = _calculate_financials(df)
    df = _apply_business_rules(df)
    df = _join_dimensions(df, dim_geo, dim_cust, dim_prod)

    df = df.sort(["order_year", "order_month", "order_day", "order_item_quantity"])
    df = df.rename({col: col.lower() for col in df.collect_schema().names()})

    return df