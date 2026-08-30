"""Fixtures for the ingestion tests.

Nine CSVs with the real Olist headers and two rows each, written to a tmp
directory and read through ``file://``. dlt's filesystem source takes the same
code path for ``file://`` and ``gs://``, so these exercise the production
loader rather than a stand-in for it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# The leading zero here is the entire point of the `text_columns` hints in
# ingestion/config.yml (§4): read as an integer it becomes 1234 and the prefix
# is silently wrong.
ZIP_WITH_LEADING_ZERO = "01234"

FIXTURES: dict[str, str] = {
    "olist_customers_dataset.csv": (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        f"c1,u1,{ZIP_WITH_LEADING_ZERO},sao paulo,SP\n"
        "c2,u2,14409,franca,SP\n"
    ),
    "olist_orders_dataset.csv": (
        "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
        "order_delivered_carrier_date,order_delivered_customer_date,"
        "order_estimated_delivery_date\n"
        "o1,c1,delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,"
        "2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00\n"
        "o2,c2,shipped,2018-07-24 20:41:37,2018-07-26 03:24:27,"
        "2018-07-26 14:31:00,,2018-08-07 00:00:00\n"
    ),
    "olist_order_items_dataset.csv": (
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "o1,1,p1,s1,2017-10-06 11:07:15,58.90,13.29\n"
        "o2,1,p2,s2,2018-07-30 03:24:27,239.90,19.93\n"
    ),
    "olist_order_payments_dataset.csv": (
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "o1,1,credit_card,1,72.19\n"
        "o2,1,boleto,1,259.83\n"
    ),
    "olist_order_reviews_dataset.csv": (
        "review_id,order_id,review_score,review_comment_title,review_comment_message,"
        "review_creation_date,review_answer_timestamp\n"
        "r1,o1,5,,,2017-10-11 00:00:00,2017-10-12 03:14:20\n"
        "r2,o2,4,,,2018-08-08 00:00:00,2018-08-09 10:22:01\n"
    ),
    "olist_products_dataset.csv": (
        "product_id,product_category_name,product_name_lenght,product_description_lenght,"
        "product_photos_qty,product_weight_g,product_length_cm,product_height_cm,"
        "product_width_cm\n"
        "p1,cama_mesa_banho,40,287,1,225,16,10,14\n"
        "p2,beleza_saude,44,276,1,1000,30,18,20\n"
    ),
    "olist_sellers_dataset.csv": (
        "seller_id,seller_zip_code_prefix,seller_city,seller_state\n"
        f"s1,{ZIP_WITH_LEADING_ZERO},campinas,SP\n"
        "s2,13844,mogi guacu,SP\n"
    ),
    "olist_geolocation_dataset.csv": (
        "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,"
        "geolocation_state\n"
        f"{ZIP_WITH_LEADING_ZERO},-23.5455,-46.6392,sao paulo,SP\n"
        "14409,-20.5097,-47.3978,franca,SP\n"
    ),
    "product_category_name_translation.csv": (
        "product_category_name,product_category_name_english\n"
        "cama_mesa_banho,bed_bath_table\n"
        "beleza_saude,health_beauty\n"
    ),
}


@pytest.fixture
def raw_csvs(tmp_path: Path) -> Path:
    """A directory holding the nine CSVs, as the GCS raw zone would."""
    csv_dir = tmp_path / "raw"
    csv_dir.mkdir()
    for name, content in FIXTURES.items():
        (csv_dir / name).write_text(content)
    return csv_dir


@pytest.fixture
def bucket_url(raw_csvs: Path) -> str:
    return raw_csvs.as_uri()


@pytest.fixture
def load_to_tmp(tmp_path: Path):
    """Run the real pipeline into a local filesystem destination.

    JSONL into a tmp directory: no duckdb, no BigQuery, no credentials, but the
    same extract and normalize stages the production load runs, so the schema
    and the written values are the real ones.
    """
    import dlt

    from ingestion.olist_source import olist_source

    def _load(bucket_url: str, source=None):
        pipeline = dlt.pipeline(
            pipeline_name="test_olist_ingest",
            destination=dlt.destinations.filesystem(str(tmp_path / "loaded")),
            dataset_name="olist_raw",
            pipelines_dir=str(tmp_path / "dlt"),
        )
        pipeline.run(source if source is not None else olist_source(bucket_url),
                     loader_file_format="jsonl")
        return pipeline

    return _load
