"""The single entrypoint.

`run_all.py` is the only way to run the pipeline by hand and the same file CI
runs nightly, so what matters here is that its flags reach the graph: what gets
selected, what config is produced, and that `--dry-run` resolves all of it
without touching Kaggle, GCS or BigQuery.

The assets themselves are tested in `test_orchestration_assets.py`; this file
is about the argument surface on top of them.
"""

from __future__ import annotations

import pytest

from ingestion.config import config
from orchestration.assets import RAW_ZONE_URI_ENV
from orchestration.definitions import all_assets
from orchestration.run_all import (
    build_parser,
    main,
    num_assets,
    plan,
    run_config,
    selection,
)


def parse(*argv: str):
    return build_parser().parse_args(list(argv))


def test_cli_accepts_the_documented_flags():
    args = parse("--bucket-url", "gs://b/2026-08-29", "--dry-run")

    assert args.bucket_url == "gs://b/2026-08-29"
    assert args.dry_run is True
    assert parse().ingest_date is None
    assert parse().bucket is None


def test_a_plain_run_selects_the_whole_graph():
    """32 keys from 4 definitions — nine dlt tables and 21 dbt models are two
    multi-assets, so counting definitions would report 4."""
    assert selection(parse()) == all_assets
    assert num_assets(all_assets) == 32


def test_bucket_url_starts_the_run_at_the_loader():
    """The raw zone is already filled, so the download and the upload are
    skipped: 30 of the 32 assets, missing kaggle_dataset and gcs_raw_files."""
    selected = selection(parse("--bucket-url", "gs://b/2026-08-29"))

    assert num_assets(selected) == 30
    keys = {k.to_user_string() for d in selected for k in d.keys}
    assert "kaggle_dataset" not in keys
    assert "gcs_raw_files" not in keys
    assert "olist_raw/olist_orders_dataset" in keys
    assert "staging/stg_orders" in keys


def test_skip_dbt_stops_at_the_raw_zone():
    """The stopgap: 11 of the 32 assets — the download, the upload and the nine
    dlt tables — and none of the 21 models."""
    selected = selection(parse("--skip-dbt"))

    assert num_assets(selected) == 11
    keys = {k.to_user_string() for d in selected for k in d.keys}
    assert "kaggle_dataset" in keys
    assert "olist_raw/olist_orders_dataset" in keys
    assert not any(k.startswith(("staging/", "intermediate/", "marts/")) for k in keys)


def test_the_two_cuts_compose():
    """They trim opposite ends and are independent, so together they leave just
    the load: no download, no upload, no models."""
    selected = selection(parse("--bucket-url", "gs://b/x", "--skip-dbt"))

    assert num_assets(selected) == 9
    keys = {k.to_user_string() for d in selected for k in d.keys}
    assert all(k.startswith("olist_raw/") for k in keys)


def test_dry_run_says_dbt_is_skipped(monkeypatch):
    monkeypatch.setenv("GCP_RAW_BUCKET", "olist-raw-test")

    assert "skipped (--skip-dbt)" in plan(parse("--skip-dbt"))
    assert "11 of 32" in plan(parse("--skip-dbt"))


def test_ingest_date_reaches_the_asset_that_reads_it():
    cfg = run_config(parse("--ingest-date", "2018-10-17"))

    assert cfg == {"ops": {"gcs_raw_files": {"config": {"ingest_date": "2018-10-17"}}}}


def test_ingest_date_is_not_passed_when_the_prefix_is_already_chosen():
    """`--bucket-url` names the prefix outright, and gcs_raw_files does not run,
    so configuring it would be config for an op not in the selection — which
    Dagster rejects."""
    assert run_config(parse("--bucket-url", "gs://b/x", "--ingest-date", "2018-10-17")) == {}


def test_dry_run_reports_the_plan(monkeypatch):
    monkeypatch.setenv("GCP_RAW_BUCKET", "olist-raw-test")

    out = plan(parse("--ingest-date", "2026-08-29"))

    assert "gs://olist-raw-test/2026-08-29" in out
    assert config().kaggle_dataset in out
    assert "olist_customers_dataset" in out
    assert "32 of 32" in out


def test_dry_run_names_the_variable_rather_than_printing_an_empty_bucket(monkeypatch):
    monkeypatch.delenv("GCP_RAW_BUCKET", raising=False)

    assert "<unset $GCP_RAW_BUCKET>" in plan(parse())


def test_dry_run_says_the_download_is_skipped(monkeypatch):
    monkeypatch.setenv("GCP_RAW_BUCKET", "olist-raw-test")

    out = plan(parse("--bucket-url", "gs://b/2026-08-29"))

    assert "gs://b/2026-08-29" in out
    assert "download skipped" in out
    assert "30 of 32" in out


def test_dry_run_materialises_nothing(monkeypatch, capsys):
    """The whole point of the flag: it must not reach Kaggle, GCS or BigQuery."""

    def explode(*args, **kwargs):
        raise AssertionError("materialize() must not be called for a dry run")

    monkeypatch.setattr("orchestration.run_all.materialize", explode)
    monkeypatch.setenv("GCP_RAW_BUCKET", "olist-raw-test")

    assert main(["--dry-run"]) == 0
    assert "olist-raw-test" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv, variable, expected",
    [
        (["--bucket", "chosen-bucket"], "GCP_RAW_BUCKET", "chosen-bucket"),
        (["--bucket-url", "gs://b/2026-08-29"], RAW_ZONE_URI_ENV, "gs://b/2026-08-29"),
    ],
)
def test_flags_reach_the_resources_through_the_environment(monkeypatch, argv, variable, expected):
    """`RawZone` reads its bucket through `EnvVar` and the loader reads the
    prefix override the same way, both at run time. Setting the variable is
    what lets a flag reach an asset without threading config through the graph.
    """
    monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr("orchestration.run_all.materialize", lambda *a, **k: _Success())

    main(argv)

    import os

    assert os.environ[variable] == expected


class _Success:
    success = True
