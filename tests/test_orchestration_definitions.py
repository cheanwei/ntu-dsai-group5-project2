"""Orchestration tests — the two ways the graph is executed (§8).

`dagster dev` loads `definitions.py`; GitHub Actions runs `run_all.py`. Both
read the same asset list, and these tests are what stop them drifting apart.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dagster import AssetKey, AssetSelection, Definitions

from ingestion.config import config
from orchestration.definitions import defs
from orchestration.run_all import build_instance
from orchestration.schedules import DAILY_CRON, DAILY_CRON_UTC, TIMEZONE, daily_refresh_schedule

WORKFLOW = Path(__file__).parent.parent / ".github" / "workflows" / "pipeline.yml"


# --- definitions.py --------------------------------------------------------


def test_the_code_location_loads_with_nothing_configured():
    """`dagster dev` must open on a laptop with no .env sourced: every value
    that needs the environment is resolved when a run starts, not at load.

    A subprocess because `defs` is built at import — clearing the variables in
    this process would prove nothing about the load that already happened.
    """
    stripped = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("GCP_", "GOOGLE_", "BIGQUERY_", "KAGGLE_", "DBT_", "DAGSTER_"))
    }

    result = subprocess.run(
        [sys.executable, "-c", "import orchestration.definitions"],
        cwd=Path(__file__).parent.parent,
        env=stripped,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_definitions_are_internally_consistent():
    """Every dep resolves to a defined asset, and every resource an asset asks
    for is supplied."""
    Definitions.validate_loadable(defs)


def test_the_whole_ingestion_chain_is_defined():
    keys = {spec.key for spec in defs.resolve_all_asset_specs()}

    assert AssetKey("kaggle_dataset") in keys
    assert AssetKey("gcs_raw_files") in keys
    assert {AssetKey(["olist_raw", table.table]) for table in config().tables} <= keys


# --- schedules.py ----------------------------------------------------------


def test_the_daily_refresh_fires_at_eight_in_the_morning_singapore_time():
    schedule = daily_refresh_schedule()

    assert schedule.cron_schedule == "0 8 * * *"
    assert schedule.execution_timezone == "Asia/Singapore"


def test_the_two_crons_name_the_same_instant():
    """Dagster resolves its cron in `TIMEZONE`; GitHub Actions has no timezone
    field and resolves in UTC. The conversion is done by hand in `schedules.py`,
    which is exactly the kind of arithmetic that is wrong by a day."""
    local = datetime(2026, 1, 1, int(DAILY_CRON.split()[1]), tzinfo=ZoneInfo(TIMEZONE))

    assert local.astimezone(UTC).hour == int(DAILY_CRON_UTC.split()[1])


def test_the_schedule_covers_every_asset():
    job = daily_refresh_schedule().target.resolvable_to_job

    assert job.selection == AssetSelection.all()


def test_the_actions_cron_matches_the_declared_schedule():
    """GitHub Actions holds the scheduler role (§8), so its cron is the one
    that actually fires. A schedule changed here and not there would be a
    declared intent the pipeline never honours."""
    crons = re.findall(r'cron:\s*"([^"]+)"', WORKFLOW.read_text())

    assert crons == [DAILY_CRON_UTC]


# --- run_all.py ------------------------------------------------------------


def test_run_history_is_kept_when_dagster_home_is_set(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))

    with build_instance() as instance:
        assert not instance.is_ephemeral


def test_an_unset_dagster_home_falls_back_rather_than_crashing(monkeypatch):
    """`DagsterInstance.get()` raises when DAGSTER_HOME is unset. On a runner
    that is destroyed after the job the history is worthless anyway (§8), so
    the run proceeds ephemerally instead of failing before it starts."""
    monkeypatch.delenv("DAGSTER_HOME", raising=False)

    with build_instance() as instance:
        assert instance.is_ephemeral


def test_run_all_imports_when_launched_as_a_path():
    """`python orchestration/run_all.py` — the form the workflow uses — puts
    `orchestration/` on sys.path rather than the repo root, so `import
    orchestration` resolves only if the module says so itself. The failure is a
    ModuleNotFoundError at the top of a scheduled run.
    """
    repo_root = Path(__file__).parent.parent
    launched_as_a_path = "import sys; sys.path[0] = 'orchestration'; import run_all"

    result = subprocess.run(
        [sys.executable, "-c", launched_as_a_path],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
