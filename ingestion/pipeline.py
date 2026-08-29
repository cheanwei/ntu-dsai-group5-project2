"""dlt pipeline: GCS raw zone -> BigQuery ``olist_raw``.

Design: architecture-design.md §4.

- ``write_disposition="replace"`` — re-running rebuilds tables instead of
  appending duplicates. Idempotence is a precondition for scheduling.
- No ``bigquery_adapter``. §5.3's partition and cluster hints are all for marts,
  and the dbt models declare them themselves — `fct_orders.sql` opens with
  ``{{ config(partition_by=..., cluster_by=...) }}``. The adapter can only
  configure the tables dlt creates, which are the raw ones, where partitioning
  ~100k rows read once per run by a staging view buys nothing.
- ``destination`` location is ``US``, set explicitly and never taken from a
  console default (§14).

dlt emits ``_dlt_loads`` and stamps ``_dlt_load_id`` on every row, so "which
run produced this row" is answerable in SQL by anyone with the Data Viewer
role granted in §15.

Invoked as a Dagster asset (orchestration/assets.py), not as a script, so it
participates in the lineage graph.

Owner: lane A1.
"""

from __future__ import annotations

import dlt
from dlt.destinations import bigquery

from ingestion.config import IngestionConfig, config
from ingestion.olist_source import olist_source


def build_pipeline(cfg: IngestionConfig | None = None):
    """Construct the dlt pipeline object (no run).

    Credentials are not passed here. dlt resolves them from the ambient
    application-default credentials, which is `GOOGLE_APPLICATION_CREDENTIALS`
    locally and the service-account key written by the workflow in CI (§10) —
    so there is no code path that could log or commit one.
    """
    cfg = cfg or config()
    return dlt.pipeline(
        pipeline_name=cfg.pipeline_name,
        destination=bigquery(location=cfg.location),
        dataset_name=cfg.dataset,
    )


def run(bucket_url: str, pipeline=None, cfg: IngestionConfig | None = None):
    """Load every source table from ``bucket_url`` into ``olist_raw``.

    Returns dlt's ``LoadInfo``. Failures are not caught: a schema-contract
    violation must fail the run so Dagster surfaces it against the asset that
    raised, rather than being logged and passed over.

    ``pipeline`` exists so tests can supply a local destination. Production
    leaves it unset.
    """
    cfg = cfg or config()
    pipeline = pipeline if pipeline is not None else build_pipeline(cfg)
    return pipeline.run(olist_source(bucket_url, cfg))


if __name__ == "__main__":
    raise SystemExit(
        "Run this through Dagster (`dagster dev`, materialise the raw assets) "
        "so the load appears in the lineage graph — see §8."
    )
