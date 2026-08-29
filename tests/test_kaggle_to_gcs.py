"""Kaggle -> GCS tests — spec §7.

No GCS client is constructed: `upload_to_gcs` takes one, so the test passes a
fake that records what it was asked to do.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.config import config
from ingestion.kaggle_to_gcs import download_dataset, upload_to_gcs

SOURCE_FILES = config().files

INGEST_DATE = "2026-08-29"


class FakeBlob:
    def __init__(self, name: str, uploads: dict[str, Path]):
        self.name = name
        self._uploads = uploads

    def upload_from_filename(self, filename, **kwargs):
        self._uploads[self.name] = Path(filename)


class FakeBucket:
    def __init__(self, name: str, uploads: dict[str, Path]):
        self.name = name
        self._uploads = uploads

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(name, self._uploads)


class FakeClient:
    def __init__(self):
        self.uploads: dict[str, Path] = {}
        self.requested_buckets: list[str] = []

    def bucket(self, name: str) -> FakeBucket:
        self.requested_buckets.append(name)
        return FakeBucket(name, self.uploads)


@pytest.fixture
def local_dir(tmp_path: Path) -> Path:
    for name in SOURCE_FILES:
        (tmp_path / name).write_text("a,b\n1,2\n")
    return tmp_path


def test_uploads_every_file_under_the_ingest_date_prefix(local_dir):
    client = FakeClient()

    upload_to_gcs(str(local_dir), "olist-raw-test", INGEST_DATE, client=client)

    assert set(client.uploads) == {f"{INGEST_DATE}/{name}" for name in SOURCE_FILES}
    assert client.requested_buckets == ["olist-raw-test"]


def test_returns_the_prefix_uri_dlt_consumes_as_bucket_url(local_dir):
    uri = upload_to_gcs(str(local_dir), "olist-raw-test", INGEST_DATE, client=FakeClient())

    assert uri == f"gs://olist-raw-test/{INGEST_DATE}"


@pytest.fixture
def kaggle_cache(tmp_path: Path):
    """Stands in for kagglehub's cache directory, which arrives unzipped."""

    def _cache(omit: tuple[str, ...] = ()) -> Path:
        cache = tmp_path / "kagglehub_cache"
        cache.mkdir(exist_ok=True)
        for name in SOURCE_FILES:
            if name not in omit:
                (cache / name).write_text("a,b\n1,2\n")
        return cache

    return _cache


def test_copies_every_source_file_into_dest_dir(tmp_path, kaggle_cache):
    cache = kaggle_cache()
    dest = tmp_path / "dest"

    result = download_dataset(str(dest), download=lambda slug: str(cache))

    assert Path(result) == dest
    assert {p.name for p in dest.iterdir()} == set(SOURCE_FILES)


def test_missing_file_is_reported_by_name(tmp_path, kaggle_cache):
    """The reason to pin a version: a version whose file set changed must fail
    here, naming what is absent, rather than load eight tables and leave the
    ninth silently empty."""
    cache = kaggle_cache(omit=("olist_sellers_dataset.csv",))

    with pytest.raises(FileNotFoundError) as excinfo:
        download_dataset(str(tmp_path / "dest"), download=lambda slug: str(cache))

    assert "olist_sellers_dataset.csv" in str(excinfo.value)


def test_downloads_the_pinned_version():
    """A bump must be a visible diff, not whatever Kaggle serves today."""
    requested: list[str] = []

    def fake_download(slug: str) -> str:
        requested.append(slug)
        raise FileNotFoundError("stop here; the slug is what is under test")

    with pytest.raises(FileNotFoundError):
        download_dataset("unused", download=fake_download)

    assert requested == [config().kaggle_dataset]
    assert "/versions/" in config().kaggle_dataset
