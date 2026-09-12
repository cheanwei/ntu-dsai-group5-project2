# Utility scripts

These scripts support local setup, one-time cloud provisioning, and isolated
ingestion debugging. The normal pipeline entrypoint is
`orchestration/run_all.py`.

## `scripts/bootstrap_env.py`

Creates `.env` from `.env.example` and writes Kaggle credentials without
printing them. Existing non-placeholder values are preserved unless `--force`
is used.

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
| `--no-oauth` | paste or redirect a token instead of minting one |
| `--no-prompt` | fail instead of prompting |
| `--force` | replace values already present in `.env` |

### Credential selection

The script chooses the first available source in this order:

1. `--token-file`
2. redirected standard input
3. interactive OAuth minting when the Kaggle CLI and a terminal are available
4. hidden token prompt
5. a legacy `kaggle.json` supplied explicitly or found in a conventional path

A supplied token always wins over minting. OAuth requires a real terminal
because the login flow displays an authorization URL and asks for a verification
code. In an IDE or wrapper without a TTY, use `--token-file` or redirect a
file.

Do not put the token directly in the command or use a literal token in shell
history. A temporary file can be deleted after the configuration is verified.

The default requested lifetime for a minted token is 30 days. Kaggle may limit
that lifetime server-side. CI should use a durable token stored as the
`KAGGLE_API_TOKEN` repository secret rather than relying on an interactive
token that can expire between scheduled runs.

### Kaggle CLI duration workaround

The implementation bypasses the `kaggle auth print-access-token --expiration`
parser because the project was developed against Kaggle CLI 2.2.4, whose
duration handling converts values such as `30d` into an invalid
`relativedelta` argument. It parses the duration locally and calls the SDK
with a `timedelta`; if that internal API fails, it warns and falls back to the
CLI's default token.

This is a compatibility workaround tied to the code and tests, not a claim that
every future Kaggle CLI release has the same defect.

### Safety guarantees

The script:

- never accepts a token as a command-line value
- uses a hidden prompt or file/standard-input read
- refuses to write `.env` unless git confirms that the file is ignored
- preserves existing configuration unless `--force` is explicit
- reports only whether a value was set or retained, never the value itself

These behaviors are covered by the Python test suite.

## `scripts/provision_gcp.sh`

Creates or validates the shared GCP data plane:

- Cloud Storage and BigQuery APIs
- a US multi-region raw bucket with uniform bucket-level access and public
  access prevention
- raw and marts BigQuery datasets
- `roles/storage.objectUser` for the pipeline service account

Run this once per project with a project administrator's `gcloud` session:

```bash
gcloud auth login
./scripts/provision_gcp.sh --dry-run
./scripts/provision_gcp.sh
```

Pass `--grant-bigquery` for a new service account that lacks project-level
`bigquery.dataEditor` and `bigquery.jobUser`.

### Why provisioning is separate

`scripts/bootstrap_env.py` is a per-developer, per-clone operation that only writes a
gitignored local file. `scripts/provision_gcp.sh` is a once-per-project operation that
creates shared cloud resources and needs administrator privileges. Combining
them would make the normal setup command fail for teammates who should not have
those privileges.

The provisioning logic stays in shell because it is a sequence of `gcloud`
and `bq` operations. Every mutation goes through the same wrapper, so
`--dry-run` prints the complete plan without making a partial change.

### Configuration and authentication

The script reads only the required keys from the repository `.env`; it does
not `source` the file. Sourcing would execute shell syntax embedded in a value
and export unrelated variables. Explicitly exported values take precedence, so
temporary overrides remain possible:

```bash
GCP_PROJECT=another-project ./scripts/provision_gcp.sh --dry-run
ENV_FILE=another.env ./scripts/provision_gcp.sh --dry-run
```

Use `gcloud auth login`, not only
`gcloud auth application-default login`. The `gcloud` and `bq` commands
use the CLI credential store, while Application Default Credentials are for
client libraries. The pipeline itself uses the service-account file named by
`GOOGLE_APPLICATION_CREDENTIALS`.

### Location and IAM boundaries

Cloud Storage bucket and BigQuery dataset locations cannot be changed in place,
and BigQuery load jobs require compatible source and destination locations.
The script therefore validates an existing resource against `GCP_LOCATION`
and fails early on a mismatch instead of allowing a later load error.

It deliberately does not:

- create dbt's staging, intermediate, or mart datasets; dbt creates target
  schemas from `transform/profiles.yml`
- grant project-wide BigQuery roles unless `--grant-bigquery` is supplied
- create a service account or mint a key

The bucket grant is `storage.objectUser`: the pipeline needs to create, list,
read, and replace raw objects, not administer the bucket.

## `scripts/run_ingestion.py`

Runs Kaggle → Cloud Storage → BigQuery without importing Dagster or dbt. Use it
only to debug the ingestion layer or when a broken dbt manifest prevents the
full asset graph from importing.

```bash
uv run python -m scripts.run_ingestion --dry-run
uv run python -m scripts.run_ingestion
uv run python -m scripts.run_ingestion \
  --bucket-url gs://<bucket>/<ingest-date>
```

`--bucket` overrides `GCP_RAW_BUCKET`; `--ingest-date` chooses the raw-zone
prefix. `--bucket-url` skips download and upload and reloads an existing
prefix.

This wrapper lives outside `ingestion/` because it deliberately combines
several steps that Dagster represents separately:

| Dagster asset | Implementation |
|---|---|
| `kaggle_dataset` | `ingestion.kaggle_to_gcs.download_dataset` |
| `gcs_raw_files` | `ingestion.kaggle_to_gcs.upload_to_gcs` |
| nine raw table assets | `ingestion.gcs_to_bigquery.run_pipeline` |

`orchestration/` imports those pieces directly so the lineage graph retains
its per-stage and per-table detail. For normal operation, use:

```bash
uv run python orchestration/run_all.py
```
