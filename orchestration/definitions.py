"""Single source of truth for the asset graph, executed two ways (§8).

    Development and demo   `dagster dev`            webserver + daemon on :3000
    Scheduled execution    GitHub Actions cron  ->  run_all.py

The asset-graph screenshot from `dagster dev` is the strongest visual available
for the Technical Overview slide.

Nothing here reads the environment at import: every value that needs it —
the raw bucket, the BigQuery dataset, the credentials — is resolved when a run
starts. So the code location loads on a laptop with no `.env` sourced, and a
missing variable fails the run that needed it with the variable named, rather
than failing the whole location with a stack trace.

Owner: lane A1.
"""

from __future__ import annotations

from dagster import Definitions

from orchestration.assets import ingestion_assets
from orchestration.resources import build_resources
from orchestration.schedules import daily_refresh_schedule

# TODO(A1): + dbt_models. TODO(B1): asset_checks=[gx_validation].
all_assets = list(ingestion_assets())

defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule()],
    resources=build_resources(),
)
