# Olist Data Platform

An end-to-end analytics pipeline for the [Olist Brazilian E-Commerce
dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): nine
CSV files, about 120 MB, and roughly 100,000 orders from 2016–2018.

```text
Kaggle → Cloud Storage → dlt → BigQuery → dbt → notebooks / dbt Docs
                         └──────── Dagster orchestration ────────┘
```

The pipeline loads a fixed source schema into `olist_raw`, builds 9 staging
views, 4 intermediate views, and 8 dimensional marts, and exposes dbt tests as
Dagster asset checks. See [Architecture](docs/architecture-design.md) for the
design and [Star schema](docs/star_schema.md) for model grains and join rules.

## Project status

The ingestion, dbt, quality, orchestration, and deployment paths are
implemented. The primary analysis is `notebooks/01_data_profiling.ipynb`.

The `dashboards/` directory and `notebooks/02_sales_trends.ipynb` are
prototypes: the Streamlit app calls an API route that is not implemented, the
Power BI directory contains no report, and notebook 02 still reads local CSVs.
Do not treat them as production consumers of the marts.

## Quick start

Prerequisites:

- Python 3.11 or 3.12
- [uv](https://docs.astral.sh/uv/)
- a GCP project with BigQuery and Cloud Storage enabled
- a service-account key with access to the project
- a Kaggle API token

Install dependencies and create local configuration:

```bash
git clone <repository-url>
cd ntu-dsai-group5-project2
uv sync
uv run nbstripout --install
uv run python scripts/bootstrap_env.py
```

`bootstrap_env.py` copies `.env.example` when needed and securely prompts for,
reads, or mints a Kaggle token. Complete these values in `.env`:

```dotenv
GCP_PROJECT=your-project-id
GCP_RAW_BUCKET=olist-raw-your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
KAGGLE_API_TOKEN=your-token
```

`.env` and service-account JSON files are ignored by git. Never commit either.

Provision the shared data plane once per GCP project:

```bash
gcloud auth login
./scripts/provision_gcp.sh --dry-run
./scripts/provision_gcp.sh
```

Use `--grant-bigquery` if the service account does not already have the needed
BigQuery roles. The script creates or validates the US multi-region raw bucket
and datasets, then grants bucket access.

## Run the pipeline

Check the resolved plan without touching external services:

```bash
uv run python orchestration/run_all.py --dry-run
```

Run the complete graph:

```bash
uv run python orchestration/run_all.py
```

Useful options:

```text
--bucket <name>             override GCP_RAW_BUCKET
--ingest-date YYYY-MM-DD    choose the Cloud Storage prefix
--bucket-url gs://...       reuse an existing prefix; skip download/upload
--skip-dbt                  stop after loading olist_raw
```

Reusing an ingest date overwrites the same objects and tables; the load uses
`write_disposition="replace"`, so reruns do not duplicate rows.

For interactive development:

```bash
uv run dagster dev -m orchestration.definitions
```

The local UI is at <http://localhost:3000>. Set `DAGSTER_HOME` to the absolute
`orchestration/` path if run history should survive process exit.

To work on dbt directly, export the variables from `.env` first:

```bash
set -a; source .env; set +a
uv run dbt build --project-dir transform --profiles-dir transform
uv run dbt docs generate --project-dir transform --profiles-dir transform
```

`DBT_TARGET` defaults to `dev`; deployed runs use `prod`.

## Verify changes

The unit suite is local and does not require network credentials:

```bash
uv run pytest        # 89 tests
uv run ruff check .
```

dbt tests run as part of `dbt build`. The complete quality inventory is in
[Data-quality tests](docs/data_quality_tests.md).

## Automation and reports

- The hosted Dagster daemon runs ingestion daily at 08:00 SGT.
- An automation-condition sensor requests dbt models after their raw inputs
  have materialized successfully.
- `.github/workflows/deploy-dagster.yml` builds and deploys the Dagster image.
- `.github/workflows/pipeline.yml` runs on demand and publishes dbt Docs to
  GitHub Pages. It is not a scheduler.

Deployment commands and security boundaries are documented in
[orchestration/deploy/README.md](orchestration/deploy/README.md).

## Repository map

| Path | Purpose |
|---|---|
| `ingestion/` | Kaggle download, Cloud Storage upload, fixed-schema dlt load |
| `transform/` | dbt sources, staging, intermediate models, marts, and tests |
| `orchestration/` | Dagster assets, resources, schedules, sensors, and CLI |
| `scripts/` | Local configuration and one-time GCP provisioning |
| `notebooks/` | Analysis notebooks; see their README for current status |
| `dashboards/` | Incomplete Flask, Streamlit, and Power BI prototypes |
| `docs/` | Architecture, quality, schema, and generated diagrams |
| `tests/` | Python unit and integration-boundary tests |

## Analytical guardrails

- Use `customer_unique_id` for customer analysis; `customer_id` is an
  order-level surrogate.
- Do not join payments directly to order items. Aggregate both to order grain
  first or use `fct_orders.payment_value_total`.
- The extract ends during 2018. Use `dim_date.is_complete_month` to avoid
  presenting the incomplete tail as a revenue collapse.
