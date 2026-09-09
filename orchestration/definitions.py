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

from orchestration.assets import ingestion_assets, transform_assets
from orchestration.resources import build_resources
from orchestration.schedules import daily_refresh_schedule
from orchestration.sensors import automation_condition_sensor

# Quality needs no entry: dbt tests arrive as asset checks on the assets they
# guard, generated from the manifest by dagster-dbt (§7).
all_assets = [*ingestion_assets(), *transform_assets()]

# Two triggers, one graph, and the split is the point (§8). The schedule pulls
# the source in on a cron because nothing external tells us when Kaggle changes;
# the sensor builds dbt off the back of the load, because that *is* an event we
# emit. Registering the sensor is not optional decoration — the automation
# conditions in assets.py are declarations that nothing evaluates without it.
defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule()],
    sensors=[automation_condition_sensor()],
    resources=build_resources(),
)
