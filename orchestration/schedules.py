"""Schedule definitions.

Design: architecture-design.md §8. This file *declares intent*; GitHub Actions
*fires* it. The `dagster-daemon` is what makes a schedule actually run, and an
unattended daemon needs an always-on machine — which is the entire cost of
hosting Dagster. The e2-micro free tier cannot hold the webserver, daemon, dbt
and dlt in 1 GB, and anything larger is real money plus a machine to patch in
the final week.

So the schedule below is the declared production intent, and
`.github/workflows/pipeline.yml` is what executes it daily at 08:00 SGT
(00:00 UTC). Report this as the deliberate simplification it is: the production
path would be Dagster+ or a container on GKE.

Owner: lane A1.
"""

from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition

# Dagster resolves its cron against `execution_timezone`, so this one is local.
DAILY_CRON = "0 8 * * *"
TIMEZONE = "Asia/Singapore"

# GitHub Actions cron is UTC-only and has no timezone field, so the same instant
# has to be written differently there: 08:00 SGT == 00:00 UTC, the same day.
# The two are asserted equivalent in tests/test_orchestration_definitions.py,
# because a schedule changed in one place and not the other is a declared intent
# the pipeline never honours.
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
        description="Full refresh of the Olist platform, 08:00 SGT.",
    )
