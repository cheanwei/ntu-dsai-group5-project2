"""Kaggle -> GCS raw zone.

Downloads the Olist dataset at a pinned version — kagglehub unzips into its own
cache and returns that directory — and uploads the nine CSVs to
``gs://<bucket>/<ingest_date>/``. Returns that URI, which
``gcs_to_bigquery.py`` consumes as its dlt ``bucket_url``.

The dataset slug, its pinned version, and the nine filenames all live in
``config.yml``.

Design: architecture-design.md §4. The raw zone exists so the exact bytes of a
run are pinned and re-loadable — it is what makes reproducibility a fact rather
than a claim. The bucket must be created with ``--location=US`` to stay
load-compatible with the US datasets (§14).

Owner: lane A1.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from ingestion.config import IngestionConfig, config


def download_dataset(dest_dir: str, download=None, cfg: IngestionConfig | None = None) -> str:
    """Download and unzip the Kaggle dataset into ``dest_dir``.

    Uses ``kagglehub``, which authenticates from ``KAGGLE_API_TOKEN`` in the
    environment. kagglehub checks that first, then ``~/.kaggle/access_token``,
    then the legacy ``KAGGLE_USERNAME``/``KAGGLE_KEY`` pair and
    ``~/.kaggle/kaggle.json``. The settings page now issues an API token, so
    the token is the path to support.

    ``download`` exists so tests can pass a stand-in for kagglehub. Production
    leaves it unset.
    """
    cfg = cfg or config()
    if download is None:
        import kagglehub

        download = kagglehub.dataset_download

    # kagglehub unzips into its own cache and hands back that directory, so
    # there is no archive left for this function to open.
    cache = Path(download(cfg.kaggle_dataset))

    missing = [name for name in cfg.csv_filenames if not (cache / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"{cfg.kaggle_dataset} did not provide: {', '.join(missing)}. "
            "The pinned version's file set has changed — check the dataset on "
            "Kaggle before bumping the pin."
        )

    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    for name in cfg.csv_filenames:
        shutil.copyfile(cache / name, dest / name)

    return str(dest)


def upload_to_gcs(
    local_dir: str,
    bucket: str,
    ingest_date: str,
    client=None,
    cfg: IngestionConfig | None = None,
) -> str:
    """Upload the CSVs to ``gs://{bucket}/{ingest_date}/`` and return that URI.

    Idempotent: re-running for the same ``ingest_date`` overwrites the same
    objects rather than accumulating copies. That is a property of using the
    same object names, not of a guard — there is nothing to clean up first.

    ``client`` exists so tests can pass a fake. In production it is left unset
    and a client is built from the ambient credentials
    (``GOOGLE_APPLICATION_CREDENTIALS``), never from anything passed in code.
    """
    cfg = cfg or config()
    if client is None:
        from google.cloud import storage

        client = storage.Client()

    source = Path(local_dir)
    gcs_bucket = client.bucket(bucket)
    for name in cfg.csv_filenames:
        blob = gcs_bucket.blob(f"{ingest_date}/{name}")
        blob.upload_from_filename(str(source / name), content_type="text/csv")

    return f"gs://{bucket}/{ingest_date}"
