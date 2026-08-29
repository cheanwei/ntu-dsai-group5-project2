"""The asset graph, end to end.

    kaggle_dataset          download at a pinned version -> GCS
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

from __future__ import annotations

# --- Layer 1: source -> raw zone (§4) --------------------------------------


def kaggle_dataset():
    """Download the Kaggle dataset at a pinned version and upload the nine CSVs
    to `gs://<bucket>/<ingest_date>/`. Returns that URI.

    TODO(A1): @asset wrapping ingestion.kaggle_to_gcs.
    """
    raise NotImplementedError("TODO(A1)")


# --- Layer 2: raw zone -> BigQuery olist_raw (§4) --------------------------


def dlt_raw_assets():
    """Nine assets, one per source table.

    TODO(A1): @dlt_assets(dlt_source=olist_source(...), dlt_pipeline=...).
    """
    raise NotImplementedError("TODO(A1)")


# --- Layer 3: dbt (§5) -----------------------------------------------------


def dbt_models():
    """21 assets generated from the dbt manifest — 9 staging, 4 intermediate,
    8 marts.

    TODO(A1): @dbt_assets(manifest=...). Map dbt sources onto the dlt asset
    keys so the two halves of the graph actually connect rather than sitting
    side by side.
    """
    raise NotImplementedError("TODO(A1)")


# --- Layer 4: quality (§7) -------------------------------------------------


def gx_validation():
    """Run the GX checkpoint against the marts and emit asset checks.

    TODO(B1): @asset_check per suite, so a payment-reconciliation failure lands
    on fct_orders rather than on a nameless task.
    """
    raise NotImplementedError("TODO(B1)")
