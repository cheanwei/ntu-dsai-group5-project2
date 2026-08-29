"""Single source of truth for the asset graph, executed two ways (§8).

    Development and demo   `dagster dev`            webserver + daemon on :3000
    Scheduled execution    GitHub Actions cron  ->  run_all.py

The asset-graph screenshot from `dagster dev` is the strongest visual available
for the Technical Overview slide.

Owner: lane A1.
"""

from __future__ import annotations

# TODO(A1):
#   from dagster import Definitions
#   defs = Definitions(
#       assets=[kaggle_dataset, dlt_raw_assets, dbt_models],
#       asset_checks=[gx_validation],
#       schedules=[daily_refresh_schedule()],
#       resources=build_resources(),
#   )
