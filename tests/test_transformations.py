import pytest
import polars as pl
from pipelines.transformations import (
    _validate_schema,
    _parse_dates,
    _safe_cast_and_filter,
    _calculate_financials,
    DataValidationError
)

def test_validate_schema_fails_with_missing_columns():
    df = pl.DataFrame({"order_year": [2021]}).lazy() # Missing almost everything
    with pytest.raises(DataValidationError):
        _validate_schema(df)

def test_safe_cast_and_filter():
    df = pl.DataFrame({
        "order_item_product_price": [10.0, -5.0, 15.0],
        "order_item_quantity": [2, 1, 0],
        "order_item_discount_rate": ["0.1", "0.2", "0.0"], # Bad type
        "order_item_profit_ratio": [0.2, 0.2, 0.2],
        "days_for_shipping_real": [2, 2, 2],
        "days_for_shipment_scheduled": [3, -1, 3] # One impossible
    }).lazy()

    res = _safe_cast_and_filter(df).collect()

    # Should drop row 2 (negative price), row 3 (quantity 0), and any with negative scheduled days
    assert res.height == 1
    assert res.select(pl.col("order_item_quantity")).item() == 2
    assert res.select(pl.col("order_item_discount_rate")).dtypes[0] == pl.Float64

def test_parse_dates():
    df = pl.DataFrame({
        "order_year": [2021, 2021],
        "order_month": [1, 2],
        "order_day": [15, 30] # Feb 30th is invalid
    }).lazy()

    result = _parse_dates(df).collect()

    assert result.height == 1
    assert result.select(pl.col("order_month")).item() == 1

def test_calculate_financials_divide_by_zero():
    # Test that actual_unit_cost doesn't explode if quantity is 0 (though filter should catch it)
    # and margin_leakage_pct handles profit+discount = 0
    df = pl.DataFrame({
        "order_item_product_price": [100.0, 100.0],
        "order_item_quantity": [0, 1], # 0 quantity
        "order_item_discount_rate": [0.0, 0.10],
                "order_item_profit_ratio": [0.0, -0.1111111111111111], # profit + discount = 0
        "days_for_shipping_real": [2, 2],
        "days_for_shipment_scheduled": [3, 3]
    }).lazy()

    res = _calculate_financials(df).collect()

    assert res.select(pl.col("actual_unit_cost"))[0].item() == 0.0
    assert res.select(pl.col("margin_leakage_pct"))[1].item() == 0.0
