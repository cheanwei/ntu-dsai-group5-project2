"""Schedule definitions.

Design: architecture-design.md §8. This file *declares intent*; GitHub Actions
*fires* it. The `dagster-daemon` is what makes a schedule actually run, and an
unattended daemon needs an always-on machine — which is the entire cost of
hosting Dagster. The e2-micro free tier cannot hold the webserver, daemon, dbt
and dlt in 1 GB, and anything larger is real money plus a machine to patch in
the final week.

So the schedule below is the declared production intent, and
`.github/workflows/pipeline.yml` is what executes it daily at 02:00 SGT
(18:00 UTC). Report this as the deliberate simplification it is: the production
path would be Dagster+ or a container on GKE.

Owner: lane A1.
"""

from __future__ import annotations

# 02:00 SGT == 18:00 UTC the previous day. The GitHub Actions cron must match.
DAILY_CRON_UTC = "0 18 * * *"
TIMEZONE = "Asia/Singapore"


def daily_refresh_schedule():
    """Materialise every asset, daily.

    Honest framing for the report (§8): the source is a static dump ending
    October 2018, so a daily run performs no new work. The pipeline is *built*
    for incremental arrival — idempotent replace loads, a date-partitioned raw
    zone — and demonstrating that on a static dataset is a deliberate
    simplification. Presenting a daily refresh as if it did real work would not
    survive Q&A.

    TODO(A1): ScheduleDefinition over AssetSelection.all().
    """
    raise NotImplementedError("TODO(A1)")
