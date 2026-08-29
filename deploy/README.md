# deploy/ — stretch goal, week 3 only

Deliberately **two services**: webserver and daemon, sharing a bind-mounted
`DAGSTER_HOME`. No database service at all — Dagster's default storage is
SQLite under `DAGSTER_HOME` (see `orchestration/dagster.yaml`), so the mount is
the storage.

Dagster's reference Compose deployment is four long-lived services plus a
per-run container — roughly 1.5–2.5 GB at peak, which rules out the only free
host (the 1 GB `e2-micro`). This file is not an attempt to host the pipeline.

    docker compose --env-file ../.env up

`DAGSTER_HOME` defaults to `../orchestration`. The webserver at
`localhost:3000` then shows the run history this machine already wrote —
the same SQLite instance `dagster dev` uses.

## What it buys

§8's closing claim — that the production path would be Dagster+ or a container
on GKE — stops being an assertion and becomes a file a marker can read. That is
the whole of it, and it is worth about two hours in week 3.

## What it does not buy, stated plainly

- **Shared history.** The instance is per-machine. A GitHub Actions runner is
  destroyed after the job and writes nowhere this mount can see, so
  scheduled-run history is not here and never was going to be without a hosted
  database (§8).
- **A working code location.** The image installs `dagster` and
  `dagster-webserver` only, not dbt/dlt/pandas, so the UI loads no definitions
  and the daemon has no schedules to tick. Both run to show the topology; the
  useful view is the history, not the asset graph. Materialise assets with
  `dagster dev` or the CI workflow.

Never week 1, where it would compete with the critical path.
