"""Ingestion config tests.

The shipped `ingestion/config.yml` is loaded, not a fixture: these assert the
file the pipeline actually runs on.
"""

from __future__ import annotations

from ingestion.config import load_config


def test_loads_the_nine_source_tables():
    config = load_config()

    assert len(config.tables) == 9
    assert len({t.table for t in config.tables}) == 9


def test_carries_the_column_hints_per_table():
    by_table = {t.table: t for t in load_config().tables}

    assert by_table["olist_customers_dataset"].text_columns == ("customer_zip_code_prefix",)
    assert "order_purchase_timestamp" in by_table["olist_orders_dataset"].timestamp_columns
    assert by_table["olist_products_dataset"].text_columns == ()


def test_pins_the_kaggle_version_and_targets_us():
    config = load_config()

    assert "/versions/" in config.kaggle_dataset
    assert config.location == "US"
    assert config.pipeline_name == "olist_ingest"


def test_env_overrides_the_configured_dataset(monkeypatch):
    monkeypatch.setenv("BIGQUERY_RAW_DATASET", "olist_raw_sandbox")

    assert load_config().dataset == "olist_raw_sandbox"


def test_env_override_is_read_when_asked_not_when_cached(monkeypatch):
    """`config()` is cached for the process. The dataset must still track the
    environment, or a sandbox override would depend on which module imported
    first."""
    from ingestion.config import config

    config()  # prime the cache with no override set
    monkeypatch.setenv("BIGQUERY_RAW_DATASET", "olist_raw_sandbox")

    assert config().dataset == "olist_raw_sandbox"


def test_unknown_key_is_rejected_by_name(tmp_path):
    import pytest

    from ingestion.config import ConfigError

    bad = tmp_path / "config.yml"
    bad.write_text(
        "kaggle: {dataset: d}\npipeline: {name: n, dataset: d, location: US}\n"
        "schema_contract: {}\ntables: []\ntypo_here: 1\n"
    )

    with pytest.raises(ConfigError) as excinfo:
        load_config(bad)

    assert "typo_here" in str(excinfo.value)
