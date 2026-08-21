import polars as pl
import s3fs
from config import S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY, SILVER_DIR

def main():
    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    # We must strip s3:// for s3fs writing sometimes depending on the version
    silver_path = SILVER_DIR.replace("s3://", "")

    dim_geo = pl.DataFrame({
        "order_state": ["CA"], "order_country": ["US"], "order_region": ["West"], "market": ["USCA"], "geo_id": [1]
    })
    dim_cust = pl.DataFrame({
        "customer_state": ["CA"], "customer_country": ["US"], "customer_geo_id": [1]
    })
    dim_prod = pl.DataFrame({
        "product_name": ["Smart watch"], "category_name": ["Sporting Goods"], "department_name": ["Fitness"], "product_key": [1]
    })

    with fs.open(f"{silver_path}/dim_geo.parquet", "wb") as f:
        dim_geo.write_parquet(f)
    with fs.open(f"{silver_path}/Dim_Customer_Geo.parquet", "wb") as f:
        dim_cust.write_parquet(f)
    with fs.open(f"{silver_path}/Dim_Product.parquet", "wb") as f:
        dim_prod.write_parquet(f)

    print("Bootstrap dimension tables created in S3 Silver.")

if __name__ == "__main__":
    main()
