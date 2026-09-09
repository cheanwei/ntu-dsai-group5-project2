"""The sensor that turns declared automation conditions into runs.

Design: architecture-design.md §8, amended here.

§8 gave the daemon one job — fire `daily_refresh` on a cron — and that cron
materialised `AssetSelection.all()`: ingestion and dbt in a single run, related
only by having started at the same instant. This module is the second job. The
schedule now covers ingestion alone, and the dbt layer is pulled by
`AutomationCondition.eager()` (orchestration/assets.py,
`OlistDbtTranslator.get_automation_condition`) once the raw tables it reads
have actually been reloaded.

The difference is not cosmetic. Under a cron, dbt runs whether or not the load
succeeded, and an ingestion failure at 08:00 produces marts rebuilt on
yesterday's raw tables and a green dbt run to go with them. Under a condition,
"the load finished" is the trigger, so a failed ingestion simply does not
produce one — the marts stay at their last good state rather than being rebuilt
from stale inputs, and the reason they did not run is the failure itself.

Worth stating plainly in the report, because the Kaggle dump is static (§4) and
a daily run performs no new work either way: what changes here is *what the
pipeline is*, not what it processes. An event-driven transform layer is the
shape a real arrival — a partner dropping a file, an hourly export — is
handled with. Demonstrating it against a fixed dataset is the same deliberate
simplification as the daily cron itself, and honest about it.

Owner: lane A1.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
)

SENSOR_NAME = "automation_conditions"

# Long enough that the daemon is not re-evaluating the whole graph continuously
# on a shared-core e2-micro (orchestration/deploy/), short enough that dbt
# starts within a minute of the load finishing rather than at the next tick of
# something that feels like a schedule again.
EVALUATION_INTERVAL_SECONDS = 60


def automation_condition_sensor() -> AutomationConditionSensorDefinition:
    """Evaluate every asset's `AutomationCondition` and request the runs.

    An `AutomationCondition` on an asset is a declaration, not a trigger:
    nothing evaluates it unless a sensor targets that asset. Dagster does
    supply an implicit one, but relying on it is the trap this project already
    hit once with schedules — see `default_status` below.

    `AssetSelection.all()` rather than the dbt assets by name, for the same
    reason `daily_refresh` selected all(): a condition added by a later lane —
    a freshness policy on the marts, an eager rebuild of a new model — is
    evaluated because it was declared, with nothing here to remember to widen.
    Assets with no condition are inert under this sensor, so covering the
    ingestion assets too costs nothing and mis-stating the selection cannot
    silently disable an asset's automation.
    """
    return AutomationConditionSensorDefinition(
        name=SENSOR_NAME,
        target=AssetSelection.all(),
        minimum_interval_seconds=EVALUATION_INTERVAL_SECONDS,
        # RUNNING, not the STOPPED default — the same argument as
        # `daily_refresh` (schedules.py), and it bites harder here. A stopped
        # schedule is a missing daily run; a stopped automation sensor is a dbt
        # layer that never runs at all, because after this change nothing else
        # triggers it. The symptom is a green ingestion run and marts that
        # quietly stop moving.
        #
        # Instance state, and Dagster only applies a default_status to a sensor
        # it has not seen before: on an instance where this was toggled off by
        # hand, that choice wins. `dagster sensor start automation_conditions`,
        # or the UI.
        default_status=DefaultSensorStatus.RUNNING,
        description="Materialises the dbt layer when its upstream raw tables are reloaded.",
    )
