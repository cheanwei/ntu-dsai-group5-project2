"""Ingestion source tests — see docs/superpowers/specs/2026-08-29-ingestion-design.md §7."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
import yaml
from conftest import FIXTURES
from dlt.pipeline.exceptions import PipelineStepFailed

from ingestion.config import config
from ingestion.olist_source import olist_source


def loaded_rows(pipeline, table: str) -> list[dict]:
    """Read back what the filesystem destination actually wrote for one table.

    dlt gzips loader files by default, hence the two globs.
    """
    root = Path(pipeline.destination_client().dataset_path)
    rows: list[dict] = []
    for path in sorted(root.glob(f"{table}/*.jsonl")):
        rows += [json.loads(line) for line in path.read_text().splitlines() if line]
    for path in sorted(root.glob(f"{table}/*.jsonl.gz")):
        with gzip.open(path, "rt") as fh:
            rows += [json.loads(line) for line in fh if line.strip()]
    return rows


SOURCES_YML = Path(__file__).parent.parent / "transform" / "models" / "staging" / "_sources.yml"


def dbt_source_identifiers() -> set[str]:
    """The physical table names dbt expects in olist_raw.

    Parsed rather than restated: this is the one contract between the loader
    and the warehouse, and a copy of it here could drift silently.
    """
    doc = yaml.safe_load(SOURCES_YML.read_text())
    (source,) = doc["sources"]
    return {t["identifier"] for t in source["tables"]}


def test_table_names_match_dbt_source_identifiers():
    assert {t.table for t in config().tables} == dbt_source_identifiers()


def test_source_exposes_one_resource_per_table_under_the_dbt_names():
    """Resource name is what dlt calls the destination table, so this is the
    same contract as above, asserted on the object dbt will actually see."""
    source = olist_source("file:///does/not/need/to/exist")

    assert set(source.resources) == dbt_source_identifiers()


def test_zip_prefix_loads_as_text_keeping_its_leading_zero(bucket_url, load_to_tmp):
    """The §4 trap. `read_csv` is pandas-backed, so an int64 parse corrupts the
    prefix before any dlt column hint is consulted — this fails unless the
    parse itself is pinned to str."""
    pipeline = load_to_tmp(bucket_url)

    columns = pipeline.default_schema.get_table_columns("olist_customers_dataset")
    assert columns["customer_zip_code_prefix"]["data_type"] == "text"

    rows = loaded_rows(pipeline, "olist_customers_dataset")
    assert {r["customer_zip_code_prefix"] for r in rows} == {"01234", "14409"}


def test_every_declared_timestamp_column_loads_as_timestamp(bucket_url, load_to_tmp):
    pipeline = load_to_tmp(bucket_url)

    wrong = {}
    for table in config().tables:
        schema = pipeline.default_schema.get_table_columns(table.table)
        for col in table.timestamp_columns:
            if schema[col]["data_type"] != "timestamp":
                wrong[f"{table.table}.{col}"] = schema[col]["data_type"]

    assert wrong == {}
    assert any(t.timestamp_columns for t in config().tables), "nothing was checked"


def test_all_null_timestamp_column_is_still_typed_and_materialized(tmp_path, load_to_tmp):
    """dlt infers types from data. A column that is empty in every row gives it
    nothing to infer from, and it drops the column with a warning — so the
    declared hint, not inference, is what keeps the raw table's shape stable
    when a load happens to carry no delivered orders."""
    csv_dir = tmp_path / "sparse"
    csv_dir.mkdir()
    for name, content in FIXTURES.items():
        (csv_dir / name).write_text(content)
    orders = FIXTURES["olist_orders_dataset.csv"].splitlines()
    header = orders[0].split(",")
    blanked = header.index("order_delivered_customer_date")
    rows = [orders[0]]
    for row in orders[1:]:
        cells = row.split(",")
        cells[blanked] = ""
        rows.append(",".join(cells))
    (csv_dir / "olist_orders_dataset.csv").write_text("\n".join(rows) + "\n")

    pipeline = load_to_tmp(csv_dir.as_uri())

    columns = pipeline.default_schema.get_table_columns("olist_orders_dataset")
    assert "order_delivered_customer_date" in columns
    assert columns["order_delivered_customer_date"]["data_type"] == "timestamp"


def test_unexpected_column_fails_the_load(raw_csvs, load_to_tmp):
    """§4's schema contract. A tenth column must fail the load rather than
    quietly widen the warehouse table.

    Two loads, because that is what the contract actually governs: the first
    establishes the schema, the second is the one that violates it. Demoing
    this live means loading twice too.
    """
    load_to_tmp(raw_csvs.as_uri())

    customers = raw_csvs / "olist_customers_dataset.csv"
    header, *rows = customers.read_text().strip().splitlines()
    customers.write_text(
        "\n".join([f"{header},loyalty_tier"] + [f"{row},gold" for row in rows]) + "\n"
    )

    with pytest.raises(PipelineStepFailed) as excinfo:
        load_to_tmp(raw_csvs.as_uri())

    assert "loyalty_tier" in str(excinfo.value)
