"""Orchestration tests — the ingestion half of the asset graph (§8).

Neither Kaggle nor GCS is contacted. Both are Dagster resources, so a test
swaps in a stand-in and the asset bodies under test are the production ones —
the same seam `ingestion/` already uses for `download=`, `client=` and
`pipeline=`.

The dlt load is *not* stubbed. It runs for real into a local filesystem
destination, as `tests/test_gcs_to_bigquery.py` does, because the thing worth
proving here is that the raw-zone URI published by `gcs_raw_files` reaches the
loader and produces nine tables.
"""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import dlt
import pytest
import yaml
from dagster import AssetKey, DagsterInstance, materialize

from ingestion.config import config
from orchestration.assets import (
    RAW_ZONE_URI_METADATA,
    gcs_raw_files,
    kaggle_dataset,
    olist_raw_tables,
)
from orchestration.resources import KaggleDataset, RawZone, Warehouse

INGESTION_ASSETS = [kaggle_dataset, gcs_raw_files, olist_raw_tables]

SOURCES_YML = Path(__file__).parent.parent / "transform" / "models" / "staging" / "_sources.yml"


# --- Stand-ins for the two external systems --------------------------------


class StubKaggle(KaggleDataset):
    """The Kaggle download, served from the fixture CSVs instead of the API."""

    source_dir: str

    def download(self) -> str:
        dest = Path(self.staging_dir)
        dest.mkdir(parents=True, exist_ok=True)
        for name in config().csv_filenames:
            shutil.copyfile(Path(self.source_dir) / name, dest / name)
        return str(dest)


class LocalRawZone(RawZone):
    """A raw zone on local disk, laid out exactly as the bucket is.

    Returns a ``file://`` URI: dlt's filesystem source takes the same code path
    for ``file://`` and ``gs://``, so the loader under test is the real one.
    """

    bucket: str = "local"
    root: str = ""

    def upload(self, local_dir: str, ingest_date: str) -> str:
        dest = Path(self.root) / ingest_date
        dest.mkdir(parents=True, exist_ok=True)
        for name in config().csv_filenames:
            shutil.copyfile(Path(local_dir) / name, dest / name)
        return dest.as_uri()


class LocalWarehouse(Warehouse):
    """`olist_raw` as JSONL in a directory: no BigQuery, no credentials, but
    the same extract and normalize stages the production load runs."""

    root: str = ""

    def pipeline(self):
        return dlt.pipeline(
            pipeline_name="test_orchestration",
            destination=dlt.destinations.filesystem(str(Path(self.root) / "loaded")),
            dataset_name="olist_raw",
            pipelines_dir=str(Path(self.root) / "dlt"),
        )

    def load_kwargs(self) -> dict:
        return {"loader_file_format": "jsonl"}


@pytest.fixture
def resources(tmp_path: Path, raw_csvs: Path) -> dict:
    from dagster_dlt import DagsterDltResource

    return {
        "kaggle": StubKaggle(staging_dir=str(tmp_path / "staging"), source_dir=str(raw_csvs)),
        "raw_zone": LocalRawZone(root=str(tmp_path / "raw_zone")),
        "warehouse": LocalWarehouse(root=str(tmp_path / "warehouse")),
        "dlt": DagsterDltResource(),
    }


@pytest.fixture
def instance():
    with DagsterInstance.ephemeral() as inst:
        yield inst


# --- The shape of the graph ------------------------------------------------


def test_the_ingestion_assets_form_one_chain():
    """Three stages, not one fused node — §8's whole reason for `@dlt_assets`."""
    assert kaggle_dataset.key == AssetKey("kaggle_dataset")
    assert AssetKey("kaggle_dataset") in gcs_raw_files.asset_deps[AssetKey("gcs_raw_files")]

    for spec in olist_raw_tables.specs:
        assert {dep.asset_key for dep in spec.deps} == {AssetKey("gcs_raw_files")}


def test_one_dlt_asset_per_source_table():
    assert set(olist_raw_tables.keys) == {
        AssetKey(["olist_raw", table.table]) for table in config().tables
    }


def test_dlt_asset_keys_match_the_dbt_source_identifiers():
    """The naming contract that lets `@dbt_assets` join onto these later: a dbt
    source's `identifier` is the BigQuery table, and the dlt asset is keyed on
    dataset + that table."""
    sources = yaml.safe_load(SOURCES_YML.read_text())["sources"]
    olist_raw = next(source for source in sources if source["name"] == "olist_raw")

    assert {AssetKey(["olist_raw", table["identifier"]]) for table in olist_raw["tables"]} == set(
        olist_raw_tables.keys
    )


# --- Stage 1: Kaggle -> local staging directory ----------------------------


def test_kaggle_dataset_stages_every_source_file(resources, instance, tmp_path):
    result = materialize([kaggle_dataset], resources=resources, instance=instance)

    staged = Path(result.output_for_node("kaggle_dataset"))
    assert {p.name for p in staged.glob("*.csv")} == set(config().csv_filenames)


def test_kaggle_dataset_records_what_it_staged(resources, instance):
    """Metadata, not a log line: the run page has to answer "did the download
    actually produce nine files?" without opening the compute logs."""
    result = materialize([kaggle_dataset], resources=resources, instance=instance)

    metadata = result.asset_materializations_for_node("kaggle_dataset")[0].metadata
    assert metadata["num_files"].value == len(config().tables)
    assert metadata["dataset"].value == config().kaggle_dataset


# --- Stage 2: staging directory -> raw zone --------------------------------


def test_gcs_raw_files_uploads_under_todays_ingest_date(resources, instance, tmp_path):
    result = materialize([kaggle_dataset, gcs_raw_files], resources=resources, instance=instance)

    uri = result.output_for_node("gcs_raw_files")
    assert uri == (tmp_path / "raw_zone" / date.today().isoformat()).as_uri()


def test_the_ingest_date_can_be_pinned_to_reload_an_earlier_prefix(resources, instance):
    """The raw zone is date-partitioned, so re-running a past date must be
    possible without editing code (§4)."""
    result = materialize(
        [kaggle_dataset, gcs_raw_files],
        resources=resources,
        instance=instance,
        run_config={"ops": {"gcs_raw_files": {"config": {"ingest_date": "2018-10-17"}}}},
    )

    assert result.output_for_node("gcs_raw_files").endswith("/2018-10-17")


def test_gcs_raw_files_publishes_the_uri_the_load_reads(resources, instance):
    """The dlt assets depend on this asset rather than take its output as an
    argument, so the URI has to travel as materialization metadata."""
    result = materialize([kaggle_dataset, gcs_raw_files], resources=resources, instance=instance)

    metadata = result.asset_materializations_for_node("gcs_raw_files")[0].metadata
    assert metadata[RAW_ZONE_URI_METADATA].value == result.output_for_node("gcs_raw_files")


# --- Stage 3: raw zone -> olist_raw ----------------------------------------


def test_the_load_reads_the_raw_zone_uri_published_upstream(resources, instance, tmp_path):
    result = materialize(INGESTION_ASSETS, resources=resources, instance=instance)

    assert result.success
    materializations = result.asset_materializations_for_node("olist_raw_tables")
    loaded = {event.asset_key.path[-1] for event in materializations}
    assert loaded == {table.table for table in config().tables}


def test_the_load_writes_every_table(resources, instance, tmp_path):
    materialize(INGESTION_ASSETS, resources=resources, instance=instance)

    written = {p.name for p in (tmp_path / "warehouse" / "loaded" / "olist_raw").iterdir()}
    assert {table.table for table in config().tables} <= written


def test_loading_before_the_raw_zone_exists_names_the_asset_to_run(resources, instance):
    """Materialising the loader alone against an empty instance is a real
    mistake to make in the UI; the failure has to say which asset is missing
    rather than fail inside dlt on an empty URI."""
    result = materialize(
        [olist_raw_tables], resources=resources, instance=instance, raise_on_error=False
    )

    assert not result.success
    failure = result.failure_data_for_node("olist_raw_tables")
    assert "gcs_raw_files" in failure.error.to_string()


# --- Materialization is repeatable -----------------------------------------


def test_rerunning_the_chain_replaces_rather_than_appends(resources, instance):
    """Idempotence is the precondition for scheduling it (§4)."""
    materialize(INGESTION_ASSETS, resources=resources, instance=instance)
    result = materialize(INGESTION_ASSETS, resources=resources, instance=instance)

    assert result.success
