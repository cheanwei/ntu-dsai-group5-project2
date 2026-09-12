# Utility scripts

These scripts support local setup and one-time provisioning. The normal
pipeline entrypoint is `orchestration/run_all.py`.

## `bootstrap_env.py`

Creates `.env` from `.env.example` and writes Kaggle credentials without
printing them. Existing non-placeholder values are preserved unless
`--force` is used.

```bash
# Interactive terminal: log in to Kaggle and request a token
uv run python scripts/bootstrap_env.py

# Read an existing token from a file
uv run python scripts/bootstrap_env.py --token-file ~/token.txt

# Import legacy username/key credentials
uv run python scripts/bootstrap_env.py ~/Downloads/kaggle.json
```

Useful options:

| Option | Effect |
|---|---|
| `--expiration 7d` | request a different OAuth token lifetime |
| `--no-oauth` | paste or pipe a token instead of minting one |
| `--no-prompt` | fail instead of prompting |
| `--force` | replace values already present in `.env` |

The script refuses to write if `.env` is not ignored by git. Delete temporary
token files after confirming the configuration works.

## `provision_gcp.sh`

Creates or validates the shared GCP data plane:

- required Cloud Storage and BigQuery APIs
- a US multi-region raw bucket
- raw and marts BigQuery datasets
- `roles/storage.objectUser` for the pipeline service account

Run this once per project with a project administrator's `gcloud` session:

```bash
gcloud auth login
./scripts/provision_gcp.sh --dry-run
./scripts/provision_gcp.sh
```

Pass `--grant-bigquery` for a new service account that lacks project-level
`bigquery.dataEditor` and `bigquery.jobUser`. The script reads only the needed
keys from `.env`; exported values take precedence.

## `run_ingestion.py`

Runs Kaggle → Cloud Storage → BigQuery without importing Dagster or dbt. Use it
only to debug the ingestion layer.

```bash
uv run python -m scripts.run_ingestion --dry-run
uv run python -m scripts.run_ingestion
uv run python -m scripts.run_ingestion \
  --bucket-url gs://<bucket>/<ingest-date>
```

`--bucket` overrides `GCP_RAW_BUCKET`; `--ingest-date` chooses the raw-zone
prefix. For normal operation, use:

```bash
uv run python orchestration/run_all.py
```
