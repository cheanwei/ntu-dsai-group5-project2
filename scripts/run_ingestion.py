"""Ingestion without Dagster: Kaggle -> GCS -> BigQuery `olist_raw`.

**Not the entrypoint.** `orchestration/run_all.py` is what a developer and CI
both run, and it covers this path plus the dbt models — the same four flags,
against the asset graph. Reach for this file only when you want the load
without Dagster in the way: debugging `ingestion/` itself, or a run on a
machine where the asset graph will not import.

    uv run python -m scripts.run_ingestion --dry-run
    uv run python -m scripts.run_ingestion
    uv run python -m scripts.run_ingestion --bucket-url gs://olist-raw-x/2026-08-29

Design: architecture-design.md §4, §8.

**Why this is here and not in `ingestion/`.** It composes what §8 models as
several distinct assets — `kaggle_dataset`, then `gcs_raw_files`, then the nine
per-table dlt assets — into a single call. That is exactly what Dagster must
not import: an asset that fused those steps would collapse the lineage graph
into one opaque node, which is the thing `@dlt_assets` and `@dbt_assets` were
chosen to avoid.

So the composition lives here. `ingestion/` keeps the pieces, each the size of
one asset:

    ingestion.kaggle_to_gcs.download_dataset      -> kaggle_dataset
    ingestion.kaggle_to_gcs.upload_to_gcs         -> gcs_raw_files
    ingestion.gcs_to_bigquery.run_pipeline        -> the nine dlt assets

Nothing in `orchestration/` imports this module, and nothing should.

Owner: lane A1.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

# Running this as a path (`python scripts/run_ingestion.py`) puts scripts/ on
# sys.path rather than the repo root, so `import ingestion` would fail with what
# looks like a broken install. The `-m` form above does not need this; the line
# is here because the other script in this folder is documented as a path and
# somebody will reasonably copy that habit.
sys.path.insert(0, str(REPO_ROOT))

# Same contract as orchestration/run_all.py, which carries the full argument:
# load .env when it is there, never over the top of the environment.
ENV_FILE = REPO_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE, override=False)

from ingestion.config import IngestionConfig, config  # noqa: E402
from ingestion.gcs_to_bigquery import run_pipeline  # noqa: E402
from ingestion.kaggle_to_gcs import download_dataset, upload_to_gcs  # noqa: E402

BUCKET_ENV = "GCP_RAW_BUCKET"


def resolve_bucket(bucket: str | None) -> str:
    """The raw-zone bucket, from the argument or the environment.

    Raises with the variable named rather than letting a `KeyError` surface
    three frames down. `.env` is already loaded by the time this runs, so
    reaching here means the variable is missing from the file rather than
    merely absent from the shell — which is a different fix, and the message
    says so.
    """
    resolved = bucket or os.environ.get(BUCKET_ENV)
    if not resolved:
        raise RuntimeError(
            f"{BUCKET_ENV} is not set and no --bucket was given. Add it to .env "
            "(see .env.example), export it into this shell, or pass --bucket."
        )
    return resolved


def stage(
    bucket: str | None = None,
    ingest_date: str | None = None,
    download=None,
    client=None,
    cfg: IngestionConfig | None = None,
) -> str:
    """Fill the raw zone: Kaggle -> local temp dir -> GCS. Returns the prefix URI.

    Separate from the load because it is separately re-runnable, and because it
    is the half that costs a 126 MB download. The temp directory is deleted
    either way — the durable copy is the one in GCS, which is the whole point of
    the raw zone (§4).
    """
    cfg = cfg or config()
    ingest_date = ingest_date or date.today().isoformat()
    with tempfile.TemporaryDirectory() as tmp:
        local_dir = download_dataset(tmp, download=download, cfg=cfg)
        return upload_to_gcs(
            local_dir, resolve_bucket(bucket), ingest_date, client=client, cfg=cfg
        )


def ingest(
    bucket: str | None = None,
    ingest_date: str | None = None,
    bucket_url: str | None = None,
    dry_run: bool = False,
    cfg: IngestionConfig | None = None,
    download=None,
    client=None,
    pipeline=None,
):
    """Download, upload and load. Returns dlt's ``LoadInfo``, or None for a dry run.

    ``bucket_url`` short-circuits the first two steps and reloads from a prefix
    already in GCS — the fast loop, since the download is most of the wall clock
    and the bytes are pinned anyway.

    ``download``, ``client`` and ``pipeline`` are injection points for tests.
    Production leaves them unset.
    """
    cfg = cfg or config()
    ingest_date = ingest_date or date.today().isoformat()

    if dry_run:
        target = bucket_url or f"gs://{resolve_bucket(bucket)}/{ingest_date}"
        print(f"kaggle    {cfg.kaggle_dataset}")
        print(f"raw zone  {target}" + ("  (existing, download skipped)" if bucket_url else ""))
        print(f"dataset   {cfg.dataset} ({cfg.location})")
        print(f"contract  {cfg.schema_contract}")
        print(f"tables    {len(cfg.tables)}")
        for table in cfg.tables:
            print(f"          {table.file} -> {table.table}")
        return None

    if bucket_url is None:
        bucket_url = stage(bucket, ingest_date, download=download, client=client, cfg=cfg)

    return run_pipeline(bucket_url, pipeline=pipeline, cfg=cfg)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.run_ingestion",
        description="Load the Olist dataset into BigQuery olist_raw.",
    )
    parser.add_argument(
        "--bucket",
        help=f"GCS raw-zone bucket. Defaults to ${BUCKET_ENV}.",
    )
    parser.add_argument(
        "--ingest-date",
        help="Raw-zone prefix, YYYY-MM-DD. Defaults to today. Re-using a date "
        "overwrites that prefix in place.",
    )
    parser.add_argument(
        "--bucket-url",
        help="Load from an existing prefix (gs://bucket/YYYY-MM-DD) and skip "
        "the Kaggle download entirely.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be loaded and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        info = ingest(
            bucket=args.bucket,
            ingest_date=args.ingest_date,
            bucket_url=args.bucket_url,
            dry_run=args.dry_run,
        )
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if info is not None:
        print(info)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
