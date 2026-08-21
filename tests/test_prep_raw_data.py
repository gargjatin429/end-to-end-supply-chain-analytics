import pytest
import polars as pl
from data_scaling.prep_raw_data import standardize_columns, normalize_text_series

def test_standardize_columns():
    df = pl.DataFrame({
        "  Product (Name) __ ": [1],
        "Customer State": [2],
        "Order_Date__": [3]
    })
    clean_df = standardize_columns(df)
    cols = clean_df.columns
    assert "product_name" in cols
    assert "customer_state" in cols
    assert "order_date" in cols

def test_normalize_text_series():
    s = pl.Series("test", ["São Paulo", "Seúl", "Japón", None, "Café"])
    normalized = normalize_text_series(s).to_list()
    assert normalized[0] == "Sao Paulo"
    assert normalized[1] == "Seul"
    assert normalized[2] == "Japon"
    assert normalized[3] is None
    assert normalized[4] == "Cafe"
