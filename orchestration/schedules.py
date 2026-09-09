"""Schedule definitions.

Design: architecture-design.md §8, as amended by `orchestration/deploy/`. This
file no longer merely declares intent — the `dagster-daemon` on `dagster-vm`
fires it.

§8 argued the opposite, and the argument was sound for the topology it
assumed: an unattended daemon needs an always-on machine, and Dagster's
reference Compose deployment (Postgres, a gRPC code server, a container per
run) does not fit in an e2-micro's 1 GB. What changed is the topology, not the
arithmetic. `orchestration/deploy/docker-compose.vm.yml` runs two services on
SQLite with in-process code locations and no per-run container — roughly
450–550 MB resident — on a host with 2 GB of swap behind it.

The consequence worth stating in the report: scheduled-run history is now
durable, which §8 said it could not be. It lives in SQLite under `DAGSTER_HOME`
on the VM, and survives redeploys and reboots.

`.github/workflows/pipeline.yml` no longer holds a cron. It is now a
reports-only workflow, run on demand to publish the dbt docs site to Pages.
Two schedulers firing `AssetSelection.all()` at the same instant would race on
the same BigQuery tables.

Owner: lane A1.
"""

from __future__ import annotations

from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition

from orchestration.assets import INGESTION_GROUP

# Dagster resolves its cron against `execution_timezone`, so this one is local.
DAILY_CRON = "0 8 * * *"
TIMEZONE = "Asia/Singapore"

# The same instant in UTC: 08:00 SGT == 00:00 UTC, the same day. Nothing fires
# on this any more — the daemon resolves DAILY_CRON in TIMEZONE directly — but
# it is still the number to write into any UTC-only scheduler (GitHub Actions
# has no timezone field), and tests/test_orchestration_definitions.py asserts
# the two stay equivalent so a change to one is not silently a change of
# meaning.
DAILY_CRON_UTC = "0 0 * * *"


def daily_refresh_schedule() -> ScheduleDefinition:
    """Pull the source into `olist_raw`, daily. The dbt layer follows on its own.

    Honest framing for the report (§8): the source is a static dump ending
    October 2018, so a daily run performs no new work. The pipeline is *built*
    for incremental arrival — idempotent replace loads, a date-partitioned raw
    zone — and demonstrating that on a static dataset is a deliberate
    simplification. Presenting a daily refresh as if it did real work would not
    survive Q&A.

    **The selection is the ingestion group, not `all()`.** It was `all()`, on
    the argument that assets added by later lanes would then be scheduled the
    moment they were defined with nothing here to update. That argument still
    holds — it just no longer needs a schedule to carry it. The dbt models
    declare `AutomationCondition.eager()` (orchestration/assets.py) and are
    pulled by the sensor in orchestration/sensors.py when the raw tables they
    read are reloaded, which reaches new models the same way and for a better
    reason: because their inputs changed, rather than because it is 08:00.

    Leaving `all()` here would not merely be redundant, it would race. The cron
    would materialise the dbt assets directly, the load inside that same run
    would satisfy their conditions, and the sensor would request a second build
    of models the first run was still writing.

    The name stays `daily_refresh` although the selection narrowed. Schedule
    enabled/disabled state is keyed by name in the instance, so renaming would
    orphan the existing entry on `dagster-vm` and leave two schedules where the
    UI shows one that has ever run.
    """
    return ScheduleDefinition(
        name="daily_refresh",
        target=AssetSelection.groups(INGESTION_GROUP),
        cron_schedule=DAILY_CRON,
        execution_timezone=TIMEZONE,
        # RUNNING, not the STOPPED default. The daemon on dagster-vm is the
        # scheduler now, and a schedule that has to be toggled on by hand in
        # the UI is one that is off after every fresh DAGSTER_HOME — which is
        # exactly the situation nobody notices until the daily run has been
        # silently absent for a week.
        #
        # This is instance state, and Dagster only applies a default_status to
        # a schedule it has not seen before. On an instance where daily_refresh
        # was already toggled off, that choice wins and this does not override
        # it: `dagster schedule start daily_refresh`, or the UI.
        default_status=DefaultScheduleStatus.RUNNING,
        description="Kaggle -> GCS -> olist_raw, 08:00 SGT. dbt follows on the load.",
    )
