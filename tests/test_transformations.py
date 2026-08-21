import pytest
import polars as pl
from pipelines.transformations import (
    _validate_schema,
    _parse_dates,
    _calculate_financials,
    _join_dimensions,
    DataValidationError,
    IncompleteDimensionError
)

def test_validate_schema_passes_with_all_columns():
    df = pl.DataFrame({
        "order_year": [2021], "order_month": [1], "order_day": [1],
        "order_item_product_price": [10.0], "order_item_quantity": [2],
        "order_item_discount_rate": [0.0], "order_item_profit_ratio": [0.2],
        "days_for_shipping_real": [2], "days_for_shipment_scheduled": [3],
        "customer_country": ["USA"], "customer_state": ["CA"],
        "order_country": ["USA"], "order_state": ["CA"],
        "order_region": ["West"], "market": ["US"],
        "product_name": ["Widget"], "category_name": ["Toys"], "department_name": ["Kids"]
    })

    # Should not raise an exception
    _validate_schema(df)

def test_validate_schema_fails_with_missing_columns():
    df = pl.DataFrame({"order_year": [2021]}) # Missing almost everything

    with pytest.raises(DataValidationError):
        _validate_schema(df)

def test_parse_dates():
    df = pl.DataFrame({
        "order_year": [2021, 2021],
        "order_month": [1, 2],
        "order_day": [15, 30] # Feb 30th is invalid
    })

    result = _parse_dates(df)

    assert result.height == 1
    assert result.select(pl.col("order_month")).item() == 1

def test_calculate_financials():
    df = pl.DataFrame({
        "order_item_product_price": [100.0],
        "order_item_quantity": [2],
        "order_item_discount_rate": [0.10], # 10% discount -> 200 * 0.1 = 20
        "order_item_profit_ratio": [0.20], # 20% of net -> 180 * 0.2 = 36
        "days_for_shipping_real": [5],
        "days_for_shipment_scheduled": [3]
    })

    res = _calculate_financials(df)

    assert res.select(pl.col("gross_sales")).item() == 200.0
    assert res.select(pl.col("discount_amount")).item() == 20.0
    assert res.select(pl.col("net_revenue")).item() == 180.0
    assert res.select(pl.col("order_profit_amount")).item() == 36.0
    assert res.select(pl.col("total_cost")).item() == 144.0
    assert res.select(pl.col("actual_unit_cost")).item() == 72.0
    assert res.select(pl.col("is_profit_bleeder")).item() == False
    assert res.select(pl.col("shipping_delta")).item() == 2

def test_join_dimensions_fails_on_null_keys():
    # Fact table with product "NewPhone"
    df = pl.DataFrame({
        "product_name": ["NewPhone"],
        "category_name": ["Electronics"],
        "department_name": ["Tech"],
        "order_state": ["CA"], "order_country": ["US"], "order_region": ["West"], "market": ["USCA"],
        "customer_state": ["CA"], "customer_country": ["US"]
    })

    # Dimension table without "NewPhone"
    dim_prod = pl.DataFrame({
        "product_name": ["OldPhone"], "category_name": ["Electronics"], "department_name": ["Tech"], "product_key": [1]
    })
    dim_geo = pl.DataFrame({
        "order_state": ["CA"], "order_country": ["US"], "order_region": ["West"], "market": ["USCA"], "geo_id": [1]
    })
    dim_cust = pl.DataFrame({
        "customer_state": ["CA"], "customer_country": ["US"], "customer_geo_id": [1]
    })

    with pytest.raises(IncompleteDimensionError):
        _join_dimensions(df, dim_geo, dim_cust, dim_prod)
