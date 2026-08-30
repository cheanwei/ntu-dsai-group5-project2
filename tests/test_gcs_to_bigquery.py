"""dlt pipeline tests — spec §7.

`build_pipeline` is asserted without credentials: constructing a BigQuery
destination configures it, it does not connect. `run` is exercised against a
local filesystem destination injected by the test, so the production function
body is the one under test.
"""

from __future__ import annotations

import dlt

from ingestion.config import config
from ingestion.gcs_to_bigquery import build_pipeline, run_pipeline


def test_targets_the_raw_dataset_in_the_us():
    pipeline = build_pipeline()

    assert pipeline.dataset_name == config().dataset
    assert pipeline.destination.destination_name == "bigquery"
    assert pipeline.destination.config_params["location"] == config().location
    assert config().location == "US"


def test_dataset_can_be_overridden_for_a_developer_sandbox(monkeypatch):
    monkeypatch.setenv("BIGQUERY_RAW_DATASET", "olist_raw_sandbox")

    assert build_pipeline().dataset_name == "olist_raw_sandbox"


def test_run_loads_every_table_from_the_bucket(bucket_url, tmp_path):
    """The real `run` body, pointed at a local destination."""
    local = dlt.pipeline(
        pipeline_name="test_run",
        destination=dlt.destinations.filesystem(str(tmp_path / "loaded")),
        dataset_name="olist_raw",
        pipelines_dir=str(tmp_path / "dlt"),
    )

    info = run_pipeline(bucket_url, pipeline=local)

    loaded = {job.job_file_info.table_name for package in info.load_packages
              for job in package.jobs["completed_jobs"]}
    assert {t for t in loaded if not t.startswith("_dlt")} == {t.table for t in config().tables}


def test_write_disposition_is_replace_so_reruns_do_not_duplicate(bucket_url, tmp_path):
    def local():
        return dlt.pipeline(
            pipeline_name="test_rerun",
            destination=dlt.destinations.filesystem(str(tmp_path / "loaded")),
            dataset_name="olist_raw",
            pipelines_dir=str(tmp_path / "dlt"),
        )

    run_pipeline(bucket_url, pipeline=local())
    pipeline = local()
    run_pipeline(bucket_url, pipeline=pipeline)

    table = pipeline.default_schema.get_table("olist_customers_dataset")
    assert table["write_disposition"] == "replace"
