"""The asset graph, end to end.

    kaggle_dataset          download at a pinned version -> local staging dir
      └─ gcs_raw_files      9 CSVs under <ingest_date>/
           └─ dlt assets    9, one per source table -> olist_raw
                └─ dbt staging (9) -> intermediate (4) -> marts (8)
                     └─ gx_validation   (asset check)

Design: architecture-design.md §8.

Two integrations do the structural work, and both were chosen for the same
reason — per-table lineage rather than one opaque node:

- `@dbt_assets` generates one Dagster asset per dbt model from the manifest.
- `dagster-dlt`'s `@dlt_assets` produces one asset per dlt resource, so the
  nine raw tables are nine nodes in the same graph as the dbt models.

GX results surface as **asset checks**, so a quality failure shows up against
the asset that produced it rather than as an unrelated task failure.

Owner: lane A1.
"""

# No `from __future__ import annotations` here: Dagster inspects the *runtime*
# annotations to find the context, config and resource parameters, and PEP 563
# turns every one of them into a string it will not resolve.

from collections.abc import Iterable
from datetime import date
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Config,
    MetadataValue,
    Output,
    asset,
)
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from ingestion.config import config as ingestion_config
from ingestion.gcs_to_bigquery import build_pipeline
from ingestion.olist_source import olist_source
from orchestration.resources import KaggleDataset, RawZone, Warehouse

INGESTION_GROUP = "ingestion"

RAW_ZONE_ASSET = AssetKey("gcs_raw_files")

# The raw-zone URI travels from `gcs_raw_files` to the loader as materialization
# metadata rather than as an asset output, because `@dlt_assets` takes upstream
# assets as *deps* — dependencies with no argument to receive a value through.
# Reading the last materialization is what makes re-running only the loader work
# at all: it picks up the prefix that was actually staged, on whatever date, and
# not a URI recomputed from today's.
RAW_ZONE_URI_METADATA = "raw_zone_uri"

# Passed to `@dlt_assets` only so the decorator can enumerate the source's nine
# resources and shape the graph. It is never opened — see `olist_raw_tables`.
SPEC_ONLY_BUCKET_URL = "gs://schema-only"


# --- Layer 1: source -> raw zone (§4) --------------------------------------


@asset(
    group_name=INGESTION_GROUP,
    kinds={"python"},
    description="The Olist dataset at its pinned Kaggle version, staged on local disk.",
)
def kaggle_dataset(kaggle: KaggleDataset) -> Output[str]:
    """Download the nine CSVs and return the directory holding them.

    Split from the upload because it is separately re-runnable and because it
    is the half that costs a 126 MB download — re-uploading a prefix should not
    pay for it twice.
    """
    staged = Path(kaggle.download())
    files = sorted(staged / name for name in ingestion_config().csv_filenames)

    return Output(
        str(staged),
        metadata={
            "dataset": ingestion_config().kaggle_dataset,
            "staging_dir": MetadataValue.path(str(staged)),
            "num_files": len(files),
            "bytes": sum(path.stat().st_size for path in files),
        },
    )


class RawZoneConfig(Config):
    """Which raw-zone prefix this materialization fills."""

    ingest_date: str | None = None  # noqa: UP045 — Dagster reads the annotation


@asset(
    group_name=INGESTION_GROUP,
    kinds={"gcs"},
    description="The nine CSVs in the GCS raw zone, under gs://<bucket>/<ingest_date>/.",
)
def gcs_raw_files(
    config: RawZoneConfig,
    raw_zone: RawZone,
    kaggle_dataset: str,
) -> Output[str]:
    """Upload the staged CSVs and return the prefix URI dlt loads from.

    Idempotent: re-running for the same `ingest_date` overwrites the same
    objects rather than accumulating copies (§4). `ingest_date` defaults to
    today and is overridable in run config, so re-loading an earlier prefix
    needs no code change — the same affordance as the CLI's `--ingest-date`.
    """
    ingest_date = config.ingest_date or date.today().isoformat()
    uri = raw_zone.upload(kaggle_dataset, ingest_date)

    return Output(
        uri,
        metadata={
            RAW_ZONE_URI_METADATA: uri,
            "ingest_date": ingest_date,
            "num_objects": len(ingestion_config().tables),
        },
    )


# --- Layer 2: raw zone -> BigQuery olist_raw (§4) --------------------------


class OlistDltTranslator(DagsterDltTranslator):
    """Keys the nine dlt resources as `olist_raw/<table>` and hangs them off the
    raw zone.

    Both departures from the default matter:

    - The default key is `dlt_<source>_<resource>`, which names the tool rather
      than the data. `olist_raw/<table>` is the BigQuery dataset and table the
      load actually writes, and it is the key `@dbt_assets` will resolve the dbt
      *sources* to — the `identifier` values in
      `transform/models/staging/_sources.yml`. Choosing it here is what lets the
      two halves of the graph join rather than sit side by side.
    - The default dep is a synthetic upstream per resource. The real upstream is
      the raw zone: nine CSVs in one prefix, loaded by one pipeline run.
    """

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        return spec.replace_attributes(
            key=AssetKey(["olist_raw", data.resource.name]),
            deps=[RAW_ZONE_ASSET],
        )


def raw_zone_uri(context: AssetExecutionContext) -> str:
    """The prefix the last `gcs_raw_files` materialization wrote.

    Materialising the loader against an empty instance is an easy mistake to
    make from the UI, so it fails here naming the asset to run rather than
    inside dlt on an empty `bucket_url`.
    """
    event = context.instance.get_latest_materialization_event(RAW_ZONE_ASSET)
    materialization = event.asset_materialization if event else None
    uri = materialization.metadata.get(RAW_ZONE_URI_METADATA) if materialization else None
    if uri is None:
        raise RuntimeError(
            "No raw zone to load from: gcs_raw_files has not been materialised in "
            "this Dagster instance. Materialise kaggle_dataset and gcs_raw_files "
            "first, or run the whole graph."
        )
    return uri.value


@dlt_assets(
    dlt_source=olist_source(SPEC_ONLY_BUCKET_URL),
    dlt_pipeline=build_pipeline(),
    name="olist_raw_tables",
    group_name=INGESTION_GROUP,
    dagster_dlt_translator=OlistDltTranslator(),
)
def olist_raw_tables(context: AssetExecutionContext, dlt: DagsterDltResource, warehouse: Warehouse):
    """Nine assets, one per source table, from one dlt pipeline run.

    The source and pipeline passed to the decorator above exist only to *shape*
    the graph — nine resources means nine asset specs, and the destination names
    the storage kind. Neither is used to load: the bucket URI is not known until
    the run, so the source is rebuilt here against the prefix the raw zone
    actually published, and the pipeline comes from the resource so it reads
    `BIGQUERY_RAW_DATASET` at run time. `SPEC_ONLY_BUCKET_URL` is never opened.
    """
    yield from dlt.run(
        context=context,
        dlt_source=olist_source(raw_zone_uri(context)),
        dlt_pipeline=warehouse.pipeline(),
        **warehouse.load_kwargs(),
    )


def ingestion_assets() -> Iterable:
    """Everything from Kaggle to `olist_raw`, in dependency order."""
    return [kaggle_dataset, gcs_raw_files, olist_raw_tables]


# --- Layer 3: dbt (§5) -----------------------------------------------------


def dbt_models():
    """21 assets generated from the dbt manifest — 9 staging, 4 intermediate,
    8 marts.

    TODO(A1): @dbt_assets(manifest=...). Map dbt sources onto the dlt asset
    keys so the two halves of the graph actually connect rather than sitting
    side by side. `OlistDltTranslator` above already keys the raw tables as
    `olist_raw/<identifier>`, which is what a `DagsterDbtTranslator` override
    for sources has to produce.
    """
    raise NotImplementedError("TODO(A1)")


# --- Layer 4: quality (§7) -------------------------------------------------


def gx_validation():
    """Run the GX checkpoint against the marts and emit asset checks.

    TODO(B1): @asset_check per suite, so a payment-reconciliation failure lands
    on fct_orders rather than on a nameless task.
    """
    raise NotImplementedError("TODO(B1)")
