"""Dagster resources: the external systems the assets talk to.

Design: architecture-design.md §8. Configuration comes from the environment —
never from a committed file. In CI the same variables are GitHub Actions
secrets (§10).

Three of the four resources are thin wrappers over `ingestion/`, and the
thinness is the point. The pipeline logic stays in `ingestion/`, where it is
tested without Dagster; what a resource adds is a **seam**. `EnvVar` resolves
at run time rather than at load, so `dagster dev` opens on a laptop with no
`.env` sourced, and a test supplies a stand-in for Kaggle, the bucket and the
warehouse without patching module attributes.

Owner: lane A1.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster import ConfigurableResource, EnvVar
from dagster_dlt import DagsterDltResource

from ingestion.gcs_to_bigquery import build_pipeline
from ingestion.kaggle_to_gcs import download_dataset, upload_to_gcs

REPO_ROOT = Path(__file__).parent.parent

# The dbt project directory. `@dbt_assets` reads the manifest emitted here, so
# there is no second DAG definition to drift out of sync (§3).
DBT_PROJECT_DIR = REPO_ROOT / "transform"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Great Expectations context (§7).
GX_PROJECT_DIR = REPO_ROOT / "quality" / "great_expectations"

# Where the Kaggle download lands before it is uploaded. Local scratch, not a
# durable artifact — the durable copy is the one in GCS, which is the whole
# point of the raw zone (§4). Under `data/`, which .gitignore already excludes
# along with every CSV, so the 126 MB cannot be committed by accident.
STAGING_DIR = REPO_ROOT / "data" / "staging"

# Named once. `scripts/run_ingestion.py` reads the same variable as `BUCKET_ENV`.
RAW_BUCKET_ENV = "GCP_RAW_BUCKET"


def gcp_project() -> str:
    return os.environ["GCP_PROJECT"]


class KaggleDataset(ConfigurableResource):
    """The Kaggle source dataset, downloaded at the pinned version (§4).

    `staging_dir` is configuration rather than a temp directory because the
    download and the upload are two assets: the second has to find what the
    first produced, and a `TemporaryDirectory` would be gone by then. It also
    makes re-uploading a download that already cost 126 MB free.
    """

    staging_dir: str

    def download(self) -> str:
        """Download and unzip into `staging_dir`. Returns that directory."""
        return download_dataset(self.staging_dir)


class RawZone(ConfigurableResource):
    """The GCS raw zone.

    The bucket must be US multi-region — one outside the US fails the load into
    a US dataset with an error that appears to blame the bucket (§14).
    """

    bucket: str

    def upload(self, local_dir: str, ingest_date: str) -> str:
        """Upload the CSVs to `gs://<bucket>/<ingest_date>/`, returning that URI."""
        return upload_to_gcs(local_dir, self.bucket, ingest_date)


class Warehouse(ConfigurableResource):
    """BigQuery `olist_raw`, reached through dlt.

    The pipeline is built per run, not once at import, so `BIGQUERY_RAW_DATASET`
    is read when the run starts rather than frozen into the code location — a
    long-lived `dagster dev` process would otherwise serve a stale dataset.
    """

    def pipeline(self):
        return build_pipeline()

    def load_kwargs(self) -> dict:
        """Extra keyword arguments for `pipeline.run`. None in production; a
        test overrides this to pick a file format its destination supports."""
        return {}


def build_resources() -> dict:
    """Resource dict passed to `Definitions`.

    `DbtCliResource` and the GX context join this dict with the assets that
    need them — `dbt_models` and `gx_validation` in `assets.py`.
    """
    return {
        "kaggle": KaggleDataset(staging_dir=str(STAGING_DIR)),
        "raw_zone": RawZone(bucket=EnvVar(RAW_BUCKET_ENV)),
        "warehouse": Warehouse(),
        "dlt": DagsterDltResource(),
    }
