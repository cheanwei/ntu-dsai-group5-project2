# orchestration/deploy/ — Dagster, running the pipeline

    docker compose --env-file ../../.env up --build

UI on `localhost:3002`. Two services off one image, both loading the code
location with `-m orchestration.definitions`:

| Service | Role |
|---|---|
| `webserver` | The UI. Submits runs, shows history and lineage. |
| `daemon` | **Executes every run**, and ticks `daily_refresh` once enabled. |

No database: Dagster's default storage is SQLite under `DAGSTER_HOME`
(`orchestration/dagster.yaml`), bind-mounted from the host.

## Three things that will bite you

**`--env-file` is required on every command.** Compose interpolates `${...}` in
`docker-compose.yml` at parse time, from the shell or `--env-file` only — the
`env_file:` key is read later, when containers start, and cannot supply a volume
path. A `.env` symlink here (`ln -s ../../.env .env`) is picked up
automatically and drops the flag, but it is gitignored, so it is per-clone.

**The daemon is not optional.** The default run coordinator is the queued one,
so a run submitted in the UI is only queued there — the daemon picks it up and
executes it. Stop the daemon and runs sit in `QUEUED` forever; restart it and
you kill whatever is running. Run logs: `docker compose logs daemon`.

**Which history you see depends on your shell.** The mount is
`${DAGSTER_HOME:-..}`, and Compose reads the shell before `--env-file`. With
`~/.zshrc` exporting `DAGSTER_HOME=$HOME/dagster_home` you get that instance;
without it you get `orchestration/`. A run you cannot find is usually this. Pin
it explicitly:

    DAGSTER_HOME=$HOME/dagster_home docker compose --env-file ../../.env up

## Runs are real, and cost real money

A full materialisation downloads 126 MB from Kaggle, writes nine CSVs to
`gs://$GCP_RAW_BUCKET/<today>/`, and replace-loads nine tables into
`$BIGQUERY_RAW_DATASET`, on your GCP bill. Idempotent (§4), but not a dry run.

The graph is `kaggle_dataset` → `gcs_raw_files` → nine `olist_raw/*` tables.
**dbt and GX are not in it yet** — `dbt_models()` and `gx_validation()` in
`assets.py` are still `TODO(A1)`/`TODO(B1)`. Both are installed in the image and
will run here once those are implemented.

`daily_refresh` (08:00 SGT, `AssetSelection.all()`) is off until you toggle it
on in the UI. GitHub Actions is still what fires the scheduled run in
production (§8).

## Credentials

Read from the same `.env` the laptop uses. The service-account key is
bind-mounted read-only from the host path in `GOOGLE_APPLICATION_CREDENTIALS`
to `/opt/dagster/secrets/gcp-sa.json` — never copied into the image, which is
also why `.dockerignore` excludes `*.json`.

The pipeline needs object-level GCS access, not bucket metadata: a
`roles/storage.objectAdmin` account returns 403 on `storage.buckets.get` and
still runs correctly.

## Volumes

`kaggle-cache` (kagglehub downloads) and `staging` (`data/staging`) are named
volumes, so a re-run does not pay the 126 MB again. `docker compose down -v`
discards both.
