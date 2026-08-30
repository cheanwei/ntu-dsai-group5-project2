"""Manual entrypoint tests.

This composes several §8 assets into one call, which is why it lives in
`scripts/` and why nothing in `orchestration/` imports it.
"""

from __future__ import annotations

from datetime import date

import dlt
import pytest

from ingestion.config import config
from scripts.run_ingestion import build_parser, ingest, resolve_bucket, stage


@pytest.fixture
def local_pipeline(tmp_path):
    return dlt.pipeline(
        pipeline_name="test_ingest",
        destination=dlt.destinations.filesystem(str(tmp_path / "loaded")),
        dataset_name="olist_raw",
        pipelines_dir=str(tmp_path / "dlt"),
    )


@pytest.fixture
def fake_kaggle(tmp_path, request):
    """A kagglehub stand-in serving the fixture CSVs, recording each call."""
    from conftest import FIXTURES

    cache = tmp_path / "kaggle_cache"
    cache.mkdir()
    for name, content in FIXTURES.items():
        (cache / name).write_text(content)

    calls: list[str] = []

    def download(slug: str) -> str:
        calls.append(slug)
        return str(cache)

    download.calls = calls
    return download


class RecordingClient:
    def __init__(self):
        self.uploads: list[str] = []

    def bucket(self, name):
        uploads = self.uploads

        class Bucket:
            def blob(self, blob_name):
                class Blob:
                    def upload_from_filename(self, filename, **kwargs):
                        uploads.append(blob_name)

                return Blob()

        return Bucket()


def test_stage_downloads_the_pinned_version_and_uploads_under_the_date(fake_kaggle):
    client = RecordingClient()

    uri = stage("olist-raw-test", "2026-08-29", download=fake_kaggle, client=client)

    assert fake_kaggle.calls == [config().kaggle_dataset]
    assert client.uploads == [f"2026-08-29/{f}" for f in config().csv_filenames]
    assert uri == "gs://olist-raw-test/2026-08-29"


def test_ingest_loads_from_whatever_stage_returned(monkeypatch, bucket_url, local_pipeline):
    """The thread between the two halves: whatever lands in the raw zone is what
    gets loaded. `stage` is redirected to the local fixture prefix so the load
    is real rather than mocked."""
    staged: list[str] = []

    def fake_stage(*args, **kwargs):
        staged.append("called")
        return bucket_url

    monkeypatch.setattr("scripts.run_ingestion.stage", fake_stage)

    ingest(bucket="olist-raw-test", pipeline=local_pipeline)

    assert staged == ["called"]
    assert local_pipeline.default_schema.get_table_columns("olist_customers_dataset")


def test_bucket_url_skips_the_download_entirely(bucket_url, fake_kaggle, local_pipeline):
    """The iteration loop: reload from a prefix already in GCS."""
    client = RecordingClient()

    ingest(bucket_url=bucket_url, download=fake_kaggle, client=client, pipeline=local_pipeline)

    assert fake_kaggle.calls == []
    assert client.uploads == []
    assert local_pipeline.default_schema.get_table_columns("olist_orders_dataset")


def test_ingest_date_defaults_to_today(fake_kaggle):
    client = RecordingClient()

    stage("b", download=fake_kaggle, client=client)

    assert client.uploads[0].startswith(f"{date.today().isoformat()}/")


def test_bucket_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("GCP_RAW_BUCKET", "olist-raw-from-env")

    assert resolve_bucket(None) == "olist-raw-from-env"


def test_missing_bucket_names_the_variable_and_the_example_file(monkeypatch):
    monkeypatch.delenv("GCP_RAW_BUCKET", raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        resolve_bucket(None)

    assert "GCP_RAW_BUCKET" in str(excinfo.value)
    assert ".env.example" in str(excinfo.value)


def test_dry_run_reports_the_plan_without_loading(capsys, fake_kaggle, local_pipeline):
    client = RecordingClient()

    result = ingest(
        bucket="olist-raw-test",
        ingest_date="2026-08-29",
        dry_run=True,
        download=fake_kaggle,
        client=client,
        pipeline=local_pipeline,
    )

    out = capsys.readouterr().out
    assert result is None
    assert fake_kaggle.calls == [] and client.uploads == []
    assert "gs://olist-raw-test/2026-08-29" in out
    assert config().kaggle_dataset in out
    assert "olist_customers_dataset" in out


def test_cli_accepts_the_documented_flags():
    args = build_parser().parse_args(["--bucket-url", "gs://b/2026-08-29", "--dry-run"])

    assert args.bucket_url == "gs://b/2026-08-29"
    assert args.dry_run is True
    assert build_parser().parse_args([]).ingest_date is None
