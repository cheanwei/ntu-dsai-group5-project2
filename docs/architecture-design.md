# Architecture

This document describes the implemented Olist data platform and the decisions
behind it. For setup and daily commands, start with the
[project README](../README.md). For production operations, see the
[Dagster deployment runbook](../orchestration/deploy/README.md).

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

Streaming, machine learning, reverse ETL, and a production dashboard are
outside the implemented scope. The source is a historical snapshot, so a
synthetic stream would add machinery without representing a real arrival
pattern.

## System flow

[Open the high-level architecture diagram](diagrams/01-high-level-architecture.excalidraw).

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
asset checks rather than as a second, manually maintained task graph.

### Why ELT

The system loads source-faithful data before transforming it in BigQuery.
At this volume, either ETL or ELT would run quickly; the important difference
is recovery. A transformation defect can be fixed in SQL and rebuilt from the
raw tables without downloading or reprocessing the Kaggle archive.

BigQuery also keeps transformation logic close to the stored data and gives
dbt a consistent execution environment. Python is used for extraction,
loading, orchestration, and analysis—not as a second transformation engine.

### Why keep a Cloud Storage raw zone

dlt can read local CSVs, but the local files are temporary and Kaggle is an
authenticated API serving an archive, not a durable filesystem. The raw zone
provides:

1. **Reproducibility:** the exact extracted files remain under a dated prefix.
2. **Replayability:** loading and transformation can be retried without Kaggle.
3. **Failure isolation:** download/upload failures are separate from warehouse
   loading and dbt failures.
4. **Source independence:** a previously landed version remains usable if the
   upstream download is unavailable.

The upload path is `gs://<bucket>/<ingest-date>/`. Reusing an ingest date
overwrites the same object names; choosing a new date preserves another raw
snapshot.

## Technology decisions

| Concern | Choice | Reason | Alternatives considered |
|---|---|---|---|
| Warehouse | BigQuery | Columnar managed warehouse, elastic compute, and alignment with the course stack | Postgres is optimized for transactional workloads; DuckDB is useful locally but would not exercise the cloud architecture |
| Ingestion | dlt | Fixed column hints, schema contracts, replace loads, automatic `_dlt_loads` ingestion-run tracking, and direct Dagster integration | Hand-written loads add retry/schema/lineage code; Meltano adds a Singer plugin boundary without removing the Kaggle download step |
| Transformation | dbt Core | Dependency graph, SQL models, tests, documentation, and model-level Dagster lineage | Raw SQL lacks dependency/test metadata; pandas and Spark are unnecessary transformation engines here |
| Orchestration | Dagster | The pipeline is naturally an asset graph and integrates directly with dlt and dbt | Airflow would duplicate dbt dependencies; cron provides no asset lineage or conditional downstream execution |
| Quality | dbt tests, dbt-utils, dbt-expectations | Structural and business checks run through one command and surface as asset checks | A separate assertion service would split ownership and reporting |
| Analysis | SQLAlchemy, pandas, notebooks | Reproducible analysis over analyst-facing marts | Direct raw-table analysis would duplicate cleaning and grain logic |

### Why dbt Core instead of dbt Cloud

Dagster already supplies scheduling, run history, logs, and orchestration for
the entire pipeline. Adding dbt Cloud would introduce a second scheduler and
split operational state between two systems.

The model-level integration is also important. `dagster-dbt` reads dbt's
manifest and exposes individual models and tests in the same asset graph as
ingestion. Treating a hosted dbt job as one opaque unit would hide that lineage.
The catalog and lineage site are generated with `dbt docs generate` and can be
published as static files by the on-demand pipeline workflow.

### Why dlt instead of Meltano

Meltano was considered because it offers declarative Singer pipelines, but it
does not eliminate the custom Kaggle step: a filesystem tap still needs the
archive downloaded and extracted first.

For this dataset, the decisive concerns are schema enforcement and integration:

- Zip-code prefixes must remain strings, while timestamps need explicit types.
  dlt derives parsing and load hints from the same declaration in
  `ingestion/config.yml`.
- The dlt schema contract fails on unexpected columns or incompatible types.
  This turns source drift into a failed run rather than a silently changed raw
  table.
- dlt records ingestion-run metadata in its `_dlt_loads` table, giving operators
  a load identifier and ingestion timing without a second tracking system.
- `dagster-dlt` exposes the nine loaded resources individually. A generic
  command wrapper would collapse that part of the lineage graph.
- dlt is one version-pinned dependency. A Singer pipeline would also require
  choosing and maintaining compatible tap and BigQuery target plugins.

Meltano would still be defensible for a team already operating a Singer stack,
but it provides no clear advantage for nine static CSV files in this project.

### Scale caveat

The source is only about 120 MB. Partitioning and clustering will not produce
a meaningful speedup for most queries at the present size. They are included
because the mart grains and access patterns would require them at a much larger
volume, and because declaring that layout now is inexpensive. They should be
described as scale-aware design, not as a performance result from this sample.

## Components

| Area | Implementation | Responsibility |
|---|---|---|
| Source | `ingestion/config.yml`, `kagglehub` | Pin the dataset version and expected files |
| Raw zone | `ingestion/kaggle_to_gcs.py` | Download and upload the nine CSVs under a dated prefix |
| Load | `ingestion/olist_source.py`, `ingestion/gcs_to_bigquery.py` | Parse declared types and replace the raw tables |
| Transform | `transform/models/` | Build staging, reusable business logic, and marts |
| Quality | dbt, dbt-utils, dbt-expectations | Check keys, relationships, domains, ranges, and invariants |
| Orchestration | `orchestration/` | Materialize assets, schedule ingestion, and trigger dbt |
| Analysis | `notebooks/01_data_profiling.ipynb` | Query the marts and present the main analysis |
| Dashboards | `dashboards/` | Streamlit dashboard over the marts (via the Flask `/api/cities` route and direct SQL); the Power BI report is authored outside this repo |
| Documentation | dbt Docs, `docs/` | Publish the catalog, lineage, and design record |

The repository mirrors these boundaries so that each layer can be developed
and tested independently. `scripts/` contains operator utilities rather than
pipeline logic; `tests/` protects Python and orchestration boundaries; and
`docs/` records the contracts that cannot be inferred from code alone.

## Ingestion contract

`ingestion/config.yml` is the source of truth for the raw load. It declares
the Kaggle version, destination, all nine files and tables, and all 52 source
columns.

The loader enforces three rules:

1. **Types are declared, not inferred.** Zip prefixes remain strings and
   nullable timestamp columns keep their intended type. The same declaration
   drives pandas parsing and dlt column hints, avoiding two schemas that can
   drift independently.
2. **Columns and data types are frozen.** An unexpected column or type change
   fails the first affected load. The table contract remains evolvable so a
   clean environment can create the nine configured tables; which tables may
   exist is constrained by the configured resources.
3. **Tables are replaced.** A rerun rebuilds each raw table instead of
   appending duplicate rows. Idempotence is required before the pipeline can
   be scheduled safely.

The raw zone separates extraction from loading. Passing `--bucket-url` replays
a successful upload without downloading from Kaggle again. A missing expected
file or a changed source schema is supposed to fail: continuing would make the
warehouse depend on an incomplete or unreviewed source version.

Physical layout is not configured during ingestion. Partition and cluster
settings belong to the dbt marts that users query, rather than to small raw
tables scanned by staging views.

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

### Transformation ownership

Cleaning happens at the earliest layer with enough context, and only once:

| Layer | Owns | Does not own |
|---|---|---|
| Ingestion | File presence, fixed source types, schema drift, repeatable loads | Business cleanup or derived metrics |
| Staging | Renames, canonical scalar types, whitespace/blank normalization, single-table deduplication | Cross-table calculations |
| Intermediate | Reusable joins and business logic such as lifecycle, customer history, geolocation, and order totals | Presentation-specific shaping |
| Marts | Fact/dimension grains, conformed keys, reporting measures, physical layout | Repairing invalid source records silently |
| Tests | Detecting structural and business-rule violations | Mutating or suppressing bad records |
| Notebooks | Analysis and communication | Reimplementing warehouse cleaning |

Nulls are interpreted in context. An absent delivery timestamp on a canceled
order can be legitimate; an absent order key is structural corruption. Models
preserve legitimate absence, derive explicit flags where useful, and let tests
surface broken invariants.

### Dimensional model

The marts form a fact constellation with four facts and four conformed
dimensions. See [Star schema](star_schema.md), which contains the dimensional
model diagram and join guidance.

Important modeling choices:

- `dim_customer` is keyed by `customer_unique_id`; raw `customer_id` changes
  between orders from the same customer.
- Payments and order items remain separate facts because both are one-to-many
  with orders. Joining them directly multiplies payment values.
- `fct_orders` is the order-grain hub for delivery performance and reconciled
  payment/item totals.
- Geolocation is reduced to one median coordinate per zip prefix and embedded
  in customer and seller dimensions.
- Large facts are partitioned by their relevant date and clustered by common
  join/filter keys.

## Data quality

Quality checks run inside `dbt build`. The repository currently defines 79 dbt
tests across sources, staging, intermediate models, and marts.

- built-in tests protect required values, accepted domains, relationships, and
  single-column keys
- dbt-utils checks composite keys and numeric expressions
- dbt-expectations checks row-count bands, date ranges, value sets, and
  timestamp ordering
- warning-severity lifecycle checks expose known source anomalies without
  blocking the build

Tests verify data; they do not repair it. A warning is reserved for a known,
quantified source anomaly whose presence should remain visible. Structural
failures such as missing keys, broken relationships, or unexpected schemas
remain blocking errors.

`DagsterDbtTranslatorSettings(enable_source_tests_as_checks=True)` maps source
and model tests onto the assets they guard. See
[Data-quality tests](data_quality_tests.md) for the current inventory.

The marts expose `payment_difference` and `is_payment_reconciled`, but there is
currently no dedicated test enforcing a reconciliation threshold. This is an
explicit coverage gap, not a documented test that silently does not exist.

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

This split models two different signals: Kaggle provides no event announcing a
new archive, so ingestion is polled on a schedule; successful raw
materialization is an event inside the platform, so transformations react to
it. Keeping the GitHub workflow unscheduled avoids a second scheduler racing
the daemon on replace loads.

### Run history and deployment topology

Without `DAGSTER_HOME`, `orchestration/run_all.py` uses an ephemeral instance and retains no
history. Local development or Compose can point `DAGSTER_HOME` at
`orchestration/`, where Dagster stores SQLite databases, event logs, and
automation state.

The hosted deployment uses one image and two services:

- `webserver` exposes the UI and submits runs
- `daemon` evaluates schedules and sensors, dequeues runs, and launches them

Both share a bind-mounted `DAGSTER_HOME`, so history survives container
restarts and redeployments. This deliberately avoids the heavier reference
topology of Postgres, a separate gRPC code server, and per-run containers. The
trade-off is that one small VM is a single point of failure and its SQLite
history is durable but neither replicated nor shared elsewhere.

## Deployment and CI

The GCP VM is an `e2-micro` with an external address for outbound Kaggle
access. Firewall rules allow SSH and the UI only through Identity-Aware Proxy
and explicitly deny public ingress. A 2 GB swap file provides headroom for the
webserver, daemon, and materialization process, but runs are slower than on a
larger machine.

Credentials are delivered during deployment and mounted read-only; they are
not copied into the image. The VM's attached service account pulls images,
while the pipeline uses the mounted key through
`GOOGLE_APPLICATION_CREDENTIALS`.

GitHub Actions has separate responsibilities:

- `.github/workflows/deploy-dagster.yml` builds a commit-SHA-tagged image and updates the VM
  after relevant changes on `main` or manual dispatch.
- `.github/workflows/pipeline.yml` runs the full graph on demand and publishes dbt Docs to
  GitHub Pages. It does not contain a schedule.

An immutable image reference matters because Compose uses the reference to
decide whether a container must be recreated. The deployment workflow writes
the new SHA into the VM environment before `compose up`.

See [Deployment](../orchestration/deploy/README.md) for IAM, networking,
cost controls, commands, and recovery procedures.

## Consumption status

The marts have three consumers:

- `notebooks/01_data_profiling.ipynb` queries the marts for the main analysis.
- `dashboards/dashboard.py` is a Streamlit app. Its geo-map page reads
  `dim_customer` through the Flask route `/api/cities` in
  `dashboards/citiesapi.py`, and its sunburst page queries
  `fct_order_items`, `dim_customer`, and `dim_product` directly.
- A Power BI report connects to the BigQuery marts. It is authored and kept
  outside this repository, so `dashboards/powerbi/` is empty.

`notebooks/02_sales_trends.ipynb` remains an exploratory prototype: it reads
local source CSVs and is excluded from the data flow in the diagram.

## Resolved infrastructure decisions

All warehouse datasets and the raw bucket use the `US` multi-region. The
source is public and anonymized, so the project has no stated data-residency
constraint. More importantly, a BigQuery load requires compatible source and
destination locations, and dataset locations cannot be changed in place.

The location is therefore explicit in provisioning, dlt, dbt profiles, and
CI. An existing resource in another location is treated as configuration drift
instead of being reused until a later load fails.
