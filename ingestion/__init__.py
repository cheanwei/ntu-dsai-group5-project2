"""Ingestion layer: Kaggle -> GCS raw zone -> dlt -> BigQuery `olist_raw`.

Four modules, in the order the data moves through them (§4):

    config.py            config.yml -> typed objects. What loads, and how.
    kaggle_to_gcs.py     download_dataset, upload_to_gcs -> the raw zone URI
    olist_source.py      olist_source -> one dlt resource per source table
    gcs_to_bigquery.py   run_pipeline -> those resources into olist_raw

Each piece is the size of one Dagster asset, and none of them composes the
others — `orchestration/assets.py` does that, and it is the only place that
does. A composition here would collapse the lineage graph into one opaque node,
which is the thing `@dlt_assets` and `@dbt_assets` were chosen to avoid.
"""
