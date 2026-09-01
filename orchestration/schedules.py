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
reports-only workflow, run on demand to publish dbt docs and GX Data Docs to
Pages. Two schedulers firing `AssetSelection.all()` at the same instant would
race on the same BigQuery tables.

Owner: lane A1.
"""

from __future__ import annotations

from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition

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
    """Materialise every asset, daily.

    Honest framing for the report (§8): the source is a static dump ending
    October 2018, so a daily run performs no new work. The pipeline is *built*
    for incremental arrival — idempotent replace loads, a date-partitioned raw
    zone — and demonstrating that on a static dataset is a deliberate
    simplification. Presenting a daily refresh as if it did real work would not
    survive Q&A.

    The selection is `all()` rather than a named list so assets added by the
    later lanes — the dbt models, the GX checks — are scheduled the moment they
    are defined, with nothing here to remember to update.
    """
    return ScheduleDefinition(
        name="daily_refresh",
        target=AssetSelection.all(),
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
        description="Full refresh of the Olist platform, 08:00 SGT.",
    )
