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


def test_declares_every_column_of_every_table():
    """The point of the fixed schema: no column is left to inference. A CSV
    column absent from `columns:` would be a new column at load time, and the
    frozen contract rejects it — so an omission here fails the load, loudly,
    rather than quietly widening the warehouse."""
    by_table = {t.table: t for t in load_config().tables}

    assert list(by_table["olist_order_items_dataset"].columns) == [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]
    assert by_table["olist_customers_dataset"].columns["customer_zip_code_prefix"] == "text"
    assert by_table["olist_orders_dataset"].columns["order_purchase_timestamp"] == "timestamp"
    assert by_table["olist_products_dataset"].columns["product_weight_g"] == "bigint"
    assert by_table["olist_order_items_dataset"].columns["price"] == "double"


def test_column_hints_are_complete_and_nullable():
    """`nullable: True` throughout. Raw is faithful to source (§6); a NOT NULL
    constraint here would reject a row the source actually contains, and
    not-null is asserted in dbt where it belongs."""
    hints = {t.table: t.column_hints for t in load_config().tables}

    items = hints["olist_order_items_dataset"]
    assert items["order_item_id"] == {"data_type": "bigint", "nullable": True}
    assert items["shipping_limit_date"] == {"data_type": "timestamp", "nullable": True}
    assert all(col["nullable"] is True for table in hints.values() for col in table.values())


def test_pandas_dtypes_pin_every_non_timestamp_column():
    """The dtype map and the dlt hints come from one declaration, so they
    cannot disagree. Timestamps are deliberately absent: pandas would parse
    them to datetime64 and hand dlt a NaT for a blank delivery date, which the
    frozen contract then rejects. Arrow-backed strings carry a real null and
    the timestamp hint does the typing."""
    by_table = {t.table: t for t in load_config().tables}

    assert by_table["olist_order_items_dataset"].pandas_dtypes == {
        "order_id": "string",
        "order_item_id": "int64[pyarrow]",
        "product_id": "string",
        "seller_id": "string",
        "price": "double[pyarrow]",
        "freight_value": "double[pyarrow]",
    }


def test_rejects_a_column_type_dlt_does_not_know(tmp_path):
    import pytest

    from ingestion.config import ConfigError

    bad = tmp_path / "config.yml"
    bad.write_text(
        "kaggle: {dataset: d}\n"
        "pipeline: {name: n, dataset: d, location: US}\n"
        "schema_contract: {columns: freeze}\n"
        "tables:\n"
        "  - file: f.csv\n"
        "    table: t\n"
        "    columns: {a: text, b: varchar}\n"
    )

    with pytest.raises(ConfigError) as excinfo:
        load_config(bad)

    assert "t.b" in str(excinfo.value)
    assert "varchar" in str(excinfo.value)


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
