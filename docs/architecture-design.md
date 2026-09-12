# Architecture

This document describes the system that is present in the repository. For
operating instructions, start with the [project README](../README.md).

## Scope

The platform processes the static Olist Brazilian e-commerce extract: nine
CSV files, 52 declared columns, and roughly 100,000 orders from 2016–2018.

Included:

- version-pinned extraction from Kaggle
- replayable raw files in Google Cloud Storage
- fixed-schema loading into BigQuery with dlt
- dbt transformations and data-quality checks
- Dagster orchestration, scheduling, and run history
- notebook analysis and dbt Docs

Streaming, machine learning, and a production dashboard are outside the
implemented scope.

## System flow

[Open the high-level architecture diagram](diagrams/01-high-level-architecture.drawio).

```text
Kaggle
  │ kagglehub
  ▼
Cloud Storage: gs://<bucket>/<ingest-date>/
  │ dlt, fixed schema, replace load
  ▼
BigQuery: olist_raw
  │ dbt
  ├─ staging       9 views
  ├─ intermediate  4 views
  └─ marts         8 tables
       │
       ├─ notebook analysis
       └─ dbt Docs
```

Dagster represents this as 32 assets: the Kaggle download, one raw-zone
prefix, nine dlt table assets, and 21 dbt model assets. dbt tests appear as
asset checks rather than separate pipeline steps.

## Components

| Area | Implementation | Responsibility |
|---|---|---|
| Source | `ingestion/config.yml`, `kagglehub` | Pin the dataset version and expected files |
| Raw zone | `ingestion/kaggle_to_gcs.py` | Download and upload the nine CSVs under a dated prefix |
| Load | `ingestion/olist_source.py`, `gcs_to_bigquery.py` | Parse declared types and replace the raw tables |
| Transform | `transform/models/` | Build staging, reusable business logic, and marts |
| Quality | dbt, dbt-utils, dbt-expectations | Check keys, relationships, domains, ranges, and invariants |
| Orchestration | `orchestration/` | Materialize assets, schedule ingestion, and trigger dbt |
| Analysis | `notebooks/01_data_profiling.ipynb` | Query the marts and present the main analysis |
| Documentation | dbt Docs, `docs/` | Publish the catalog, lineage, and design record |

## Ingestion contract

`ingestion/config.yml` is the source of truth for the raw load. It declares
the Kaggle version, destination, all nine files and tables, and all 52 source
columns.

The loader enforces three rules:

1. **Types are declared, not inferred.** Zip prefixes remain strings and
   nullable timestamp columns keep their intended type.
2. **Columns and data types are frozen.** An unexpected column or type change
   fails the first affected load. New configured tables may still be created.
3. **Tables are replaced.** A rerun rebuilds each raw table instead of
   appending duplicate rows.

The raw zone separates extraction from loading. `--bucket-url` can replay a
successful upload without downloading from Kaggle again.

## Warehouse design

The production dbt target creates these BigQuery datasets:

| Dataset | Materialization | Purpose |
|---|---|---|
| `olist_raw` | tables loaded by dlt | Source-faithful data plus dlt metadata |
| `olist_staging` | views | Rename fields, normalize types, and deduplicate reviews |
| `olist_intermediate` | views | Geolocation, order lifecycle, payment totals, and customer history |
| `olist_marts` | tables | Analyst-facing dimensions and facts |

The development target uses `dbt_dev_*` datasets. Dataset locations are fixed
to BigQuery's `US` multi-region.

The marts form a fact constellation with four facts and four conformed
dimensions. See [Star schema](star_schema.md) and the
[dimensional diagram](diagrams/02-warehouse-dimensional-model.drawio).

Important modeling choices:

- `dim_customer` is keyed by `customer_unique_id`; raw `customer_id` changes
  between orders from the same customer.
- Payments and order items remain separate facts because both are one-to-many
  with orders. Joining them directly multiplies payment values.
- Geolocation is reduced to one median coordinate per zip prefix and embedded
  in customer and seller dimensions.
- Large facts are partitioned by their relevant date and clustered by common
  join/filter keys. This expresses the intended access pattern; it is not a
  performance claim for a 120 MB sample.

## Data quality

Quality checks run inside `dbt build`. The repository currently defines 79 dbt
tests across sources, staging, intermediate models, and marts.

- built-in tests protect required values and single-column keys
- dbt-utils checks composite keys and numeric expressions
- dbt-expectations checks row-count bands, date ranges, value sets, and
  timestamp ordering
- warning-severity lifecycle checks expose known source anomalies without
  blocking the build

`DagsterDbtTranslatorSettings(enable_source_tests_as_checks=True)` maps source
and model tests onto the assets they guard. See
[Data-quality tests](data_quality_tests.md) for the concise inventory.

## Orchestration

There is one asset graph and two ways to run it:

- `orchestration/run_all.py` materializes a selected graph in one process.
- `orchestration.definitions` serves the same graph to Dagster's UI and daemon.

The hosted trigger sequence is:

```text
08:00 SGT daily_refresh
        │
        ▼
Kaggle → GCS → olist_raw
        │ successful raw materializations
        ▼
automation_conditions sensor → dbt models
```

The schedule selects only the ingestion group. Each dbt model declares an
eager automation condition, evaluated by a sensor every 60 seconds. Failed
ingestion therefore does not request a transform run over stale raw data.

## Deployment and CI

The hosted Dagster deployment uses one image and two services:

- `webserver` exposes the UI
- `daemon` evaluates schedules and sensors and launches queued runs

Both services share a bind-mounted `DAGSTER_HOME`, so SQLite run history and
automation state survive container restarts. The deployment intentionally has
no Postgres or separate gRPC code server.

The GCP VM is an `e2-micro` with an external address for outbound access.
Firewall rules allow SSH and the UI only through Identity-Aware Proxy and deny
public ingress. Credentials are delivered during deployment and mounted
read-only; they are not copied into the image.

GitHub Actions has separate responsibilities:

- `deploy-dagster.yml` builds a SHA-tagged image and updates the VM after
  relevant changes on `main` or manual dispatch.
- `pipeline.yml` runs the full graph on demand and publishes dbt Docs to
  GitHub Pages. It does not contain a schedule.

See [Deployment](../orchestration/deploy/README.md) for commands and recovery
steps.

## Consumption status

`notebooks/01_data_profiling.ipynb` is the implemented marts-based analysis.
Other consumer work is incomplete:

- `notebooks/02_sales_trends.ipynb` reads local source CSVs and contains an
  exploratory BigQuery write path.
- `dashboards/dashboard.py` calls `/api/cities`, which `dashboards/api.py`
  does not provide.
- `dashboards/powerbi/` contains no `.pbix` report.

These files remain prototypes and are excluded from the production data flow
in the diagrams.

## Constraints and risks

| Risk | Control or accepted limitation |
|---|---|
| Source schema changes silently | Fixed column and type contract fails the load |
| Reruns duplicate data | Dated object names plus replace loads |
| Payment-to-item fan-out | Separate facts and order-grain reconciliation |
| Wrong customer identity | Mart key and test use `customer_unique_id` |
| Incomplete 2018 tail looks like decline | `dim_date.is_complete_month` flags safe periods |
| Credentials enter git or images | Ignore rules, Actions secrets, runtime mounts |
| Hosted history is lost with the VM | Accepted: SQLite is durable but not replicated |
| Static source is refreshed daily | Deliberate demonstration of an arrival-driven design |
