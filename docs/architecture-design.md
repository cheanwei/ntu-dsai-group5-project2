# Olist Data Platform — Architecture and Design

**Project:** Module 2 Assignment — Brazilian E-Commerce Dataset by Olist

---

## 1. Context and scope

Build an end-to-end data platform over the [Olist Brazilian E-Commerce
dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): nine CSVs,
about 120 MB, covering roughly 100k orders placed between September 2016 and
October 2018.

**In scope** — ingestion, a dimensional warehouse, an ELT pipeline, data quality
testing, Python analysis, orchestration, documentation, and an executive
presentation.

**Out of scope** — streaming (the source is a static historical dump; a synthetic
stream would add machinery without adding truth), ML models, and any dashboard
beyond the stretch goal in §11.

**Success is graded on** pipeline accuracy and integrity, code quality,
architecture and scalability, and documentation quality — with depth of analysis
as a secondary criterion. Design decisions below are argued against those four
criteria, not against general good taste.

---

## 2. Architecture overview

See `docs/diagrams/01-high-level-architecture.excalidraw`.

```
Kaggle CSVs → GCS raw zone → dlt → BigQuery → dbt (staging → intermediate → marts)
                                       ↓                        ↓
                              Great Expectations        SQLAlchemy + pandas
                                                          (Jupyter notebooks)

              all of it orchestrated as Dagster software-defined assets
```

Five layers, each with one job, plus orchestration spanning all of them:

| Layer | Technology | Responsibility |
|---|---|---|
| Source | Kaggle API | Fetch the dataset at a pinned version |
| Raw zone | Google Cloud Storage | Immutable, date-partitioned landing zone |
| Ingestion | dlt | Typed load into BigQuery, idempotent |
| Warehouse | BigQuery + dbt | Transform raw into a queryable dimensional model |
| Consumption | SQLAlchemy + pandas | Analysis, notebooks, exported charts |
| Orchestration | Dagster | One lineage graph across all of the above |

### Why ELT rather than ETL

Load raw first, transform in the warehouse. BigQuery's compute is elastic and
cheap at this volume; a laptop's is neither. The decisive argument is not
performance but *re-runnability*: when a bug surfaces in the delivery-lag logic
in week three, an ELT pipeline fixes SQL and rebuilds in seconds, while an ETL
pipeline re-downloads and re-processes from source. The raw zone is what makes
that guarantee real.

### Why a GCS raw zone rather than loading straight to BigQuery

1. **Reproducibility.** The exact bytes are pinned under `ingest_date=YYYY-MM-DD/`,
   so a grader can rebuild the warehouse from scratch and get identical numbers.
2. **Failure isolation.** Ingestion failures and transformation failures surface in
   different places, so "which layer broke" is answerable without a bisect.
3. **Source independence.** If Kaggle changes or removes the dataset, the pipeline
   still runs.

Cost at this volume is roughly one cent per month.

---

## 3. Technology decisions

| Decision | Chosen | Rejected alternatives | Rationale |
|---|---|---|---|
| Warehouse | **BigQuery** | DuckDB; Supabase/Postgres | Columnar and separates storage from compute, so the scalability story is genuine rather than asserted. Matches the course stack. 120 MB sits well inside the free tier. Postgres is an OLTP engine and would need defending; DuckDB is excellent locally but weakens the cloud-architecture narrative. |
| Ingestion | **dlt** | Hand-rolled Python; Meltano; BigQuery external tables | ~40 lines rather than ~150, with Google's recommended retry and timeout behaviour already wired. Emits `_dlt_loads` lineage tables for free — direct evidence for the pipeline-integrity criterion. One maintained BigQuery destination, and `bigquery_adapter` available for physical table options if a raw table ever needs them. Meltano's Singer layer adds version-pinning fragility and buys nothing on nine static local files. |
| Transformation | **dbt Core** | dbt Cloud; raw SQL scripts; Spark; pandas | Dependency resolution, testing, and documentation in one tool. Spark is unjustifiable at 120 MB and would be a red flag, not a strength. pandas transformations do not scale and cannot be tested declaratively. |
| Quality | **dbt tests + Great Expectations** | Either alone | Two different jobs — see §7. |
| Orchestration | **Dagster** | Airflow; cron; GitHub Actions | The pipeline *is* a graph of tables, which is exactly Dagster's asset model. `@dbt_assets` reads the dbt manifest and generates one Dagster asset per dbt model, so there is no second DAG definition to drift out of sync. Airflow would require hand-writing dependencies dbt already knows. Cron gives no lineage and no observability. |
| Analysis | **SQLAlchemy + pandas** | Direct BigQuery client; Streamlit | Specified by the brief; keeps the warehouse swappable behind a dialect. |

### Why dbt Core rather than dbt Cloud

dbt Cloud's principal offering is a hosted scheduler with run history, logging,
and notifications — which is precisely the role Dagster already fills, and fills
more broadly, since Dagster also covers ingestion and quality in the same graph.
Adopting both would mean two schedulers and a lineage graph split across two
tools.

The integration argument is decisive. `dagster-dbt`'s `@dbt_assets` reads the
`manifest.json` emitted by dbt Core and generates one Dagster asset per model.
Dagster's dbt Cloud integration instead orchestrates dbt Cloud *jobs* as opaque
units, collapsing model-level lineage into a single node — surrendering the exact
capability that decided Dagster over Airflow.

Everything else dbt Cloud sells is either free in Core or irrelevant at this
scale: `dbt docs generate` produces the lineage graph and catalog as static HTML
that publishes to GitHub Pages at no cost; the browser IDE is redundant when the
deliverable is a local repository; and the semantic layer, dbt Mesh, and RBAC
address organisational problems a four-person project does not have. Against
that, an additional account, connected repository, and managed environment add
credential surface and failure modes in the final week.

### The honest caveat about scale

Olist is 120 MB. Partitioning and clustering will not measurably speed up a single
query at this size, and claiming otherwise invites a fair challenge in Q&A. They
are declared because the *grain and access pattern* would demand them at 1000×,
and because the cost of designing for that now is zero. The correct framing for
the report and the presentation: **the platform is designed for the scale the
business would reach, demonstrated on a sample.** State the sample size openly.

---

## 4. Ingestion design

A dlt pipeline reading from GCS and loading to the `olist_raw` dataset.

**Non-negotiables:**

- **Explicit column hints, not schema inference.** Two known traps in this dataset:
  `customer_zip_code_prefix` and `seller_zip_code_prefix` carry leading zeros and
  must be `STRING` — inferred as `INT64` they are silently corrupted. The
  `order_*_timestamp` and `*_date` columns need explicit `TIMESTAMP` typing.
- **Schema contract set to `freeze`.** Unexpected columns or type drift fail the
  load rather than silently reshaping the warehouse.
- **`write_disposition="replace"`.** Re-running the pipeline rebuilds tables
  instead of appending duplicates. Idempotence is a precondition for scheduling.
- **GCS as staging**, preserving the raw zone described in §2.
- **The physical design is declared in the dbt models, not at ingest.** §5.3's
  partition and cluster hints are all marts, and each mart model carries its own
  `{{ config(partition_by=..., cluster_by=...) }}`. dlt's `bigquery_adapter` can
  only configure the tables dlt itself creates — the raw ones — where
  partitioning ~100k rows that a staging view full-scans once per run buys
  nothing measurable.

### Why GCS, given that dlt reads CSVs directly

dlt reads CSVs from a filesystem; Kaggle is not a filesystem but an authenticated
API serving a zip archive. A download-and-unzip step therefore always precedes
dlt, and the only real question is where the extracted CSVs live afterwards.

dlt's API is identical for either answer — `bucket_url="file:///tmp/olist"` and
`bucket_url="gs://olist-raw/2026-08-24"` differ by one string — so GCS costs
nothing in pipeline code. The three candidates:

| Where | Assessment |
|---|---|
| Local temp directory | Simplest, but the files vanish between runs, making every run dependent on Kaggle availability and a valid token, with no way to prove which version was loaded |
| Committed to git | 120 MB in a repository. Rejected |
| **GCS** | Durable, dated, re-loadable, read by dlt with the same one-liner |

GCS is therefore not ceremony but the answer to *where the CSVs live between
runs*, given they cannot go in git and cannot be reliably re-fetched. It is what
makes the raw zone a fact rather than a claim.

The transfer is a single Dagster asset: `kagglehub` downloads and unzips, then
each CSV is uploaded to `gs://olist-raw-<project>/<ingest_date>/`. The asset
returns that URI, which the downstream dlt asset consumes as its `bucket_url`.
`KAGGLE_API_TOKEN` is supplied from the environment. The Kaggle settings page
now issues an API token string rather than a `kaggle.json` download, and
kagglehub resolves that variable first — `KAGGLE_USERNAME`/`KAGGLE_KEY` and
`kaggle.json` still work but are marked legacy in its own source.

**Accepted fallback:** dropping GCS and reading a local path is defensible under
time pressure. It forfeits the reproducibility guarantee and reduces the raw zone
to a temp directory; it saves roughly an hour. Not recommended.

### Meltano reconsidered

Meltano was re-evaluated on the specific question of whether it removes the
Kaggle download step. It does not: `tap-csv` reads CSVs from a filesystem and
cannot authenticate to Kaggle or unzip an archive, exactly as dlt cannot. The
download step is orthogonal to the EL tool and costs the same ~15 lines either
way.

Setting Kaggle aside, four arguments decide it. The first three concern this
dataset specifically; the fourth concerns consistency with a decision already
taken in §3.

**1. There is no first-party `target-bigquery`.** Meltano Hub lists several
competing community loaders, and they do not merely differ in quality — they
expose *different configuration keys for the same concept*:

| Variant | How a partition is declared |
|---|---|
| default / `z3z1ma` | `partition_granularity`, plus `cluster_on_key_properties` (clusters on `_sdc_batched_at` when false) |
| `adswerve` | separate implementation, separate load method |
| `youcruit` | `default_partition_column`, or per-stream `table_configs` |

Selecting a variant is therefore an irreversible-ish decision taken early, with
no authoritative default, and switching later means rewriting configuration
rather than changing a flag. dlt has one BigQuery destination, maintained by
dlthub, with `bigquery_adapter` as the single API for physical table options
should a raw table need them. The physical design in §5.3 belongs to the dbt
models and is expressed the same way regardless of who implements it.

**2. Type control in Singer is a two-party negotiation.** The leading-zero trap
in `customer_zip_code_prefix` and `seller_zip_code_prefix` is the load's one
genuine correctness risk. In dlt it is a dict adjacent to the resource:
`columns={"customer_zip_code_prefix": {"data_type": "text"}}`. In Meltano the
tap must emit a corrected catalog schema *and* the chosen target must honour it
rather than re-infer — two components, from two maintainers, either of which can
be the reason a zip prefix arrives as `INT64`. Correctness that depends on an
agreement between two plugins is harder to defend than correctness declared in
one place.

**3. Singer has no schema contract.** dlt's
`schema_contract={"columns": "freeze", "data_type": "freeze"}` raises
`DataValidationError` and **fails the load** on an unexpected column or a type
change. Singer's specification has no equivalent notion; targets generally
evolve the destination table silently — adding the column, widening the type.
The §6 rule that raw must stay faithful to source is, under dlt, enforced by the
tool; under Meltano it is a convention nobody checks. This is the clearest
single piece of evidence available for the pipeline-integrity criterion, and it
can be demonstrated live by feeding the loader a tenth column and showing the
run fail.

**4. Choosing Meltano would contradict the reason Dagster was chosen.**
`dagster-dlt` is the official integration: `@dlt_assets` and
`DagsterDltResource` produce one Dagster asset per dlt resource, so the nine raw
tables appear as nine nodes in the same lineage graph as the dbt models.
`dagster-meltano` is community-maintained and in practice shells out to
`meltano run`, collapsing all nine loads into one opaque op. §3 rejected dbt
Cloud for exactly this — orchestrating jobs as opaque units "collapsing
model-level lineage into a single node." Rejecting dbt Cloud on that ground
while accepting Meltano on the same ground would be incoherent, and a marker
would be right to say so.

Two supporting points, neither decisive alone:

- **Lineage lives in the warehouse.** dlt writes `_dlt_loads` and stamps
  `_dlt_load_id` on every row, so "which run produced this row" is answerable in
  SQL, inside BigQuery, by anyone with the Data Viewer role granted in §15.
  Meltano's state lives in a separate state backend, and `_sdc_*` metadata
  columns depend on the target variant.
- **CI footprint.** `meltano install` resolves and builds a virtualenv per plugin
  from the Hub at run time, putting dependency resolution and a network
  dependency inside the GitHub Actions job of §8. dlt is one version-pinned
  entry in `pyproject.toml`.

The residual comparison, for completeness:

| | Meltano | dlt |
|---|---|---|
| Declaration | `meltano.yml` plus per-file entity config for nine files | one `filesystem(file_glob="*.csv") \| read_csv()` covering all nine |
| Reading from `gs://` | `tap-csv` reads a local filesystem; GCS requires `tap-spreadsheets-anywhere` or a download-first step — **verify before committing to Meltano**, since §4's raw zone depends on it | `bucket_url="gs://…"`, natively |
| Install footprint | Meltano project, plugin virtualenvs, state backend | one dependency |

The intuition that YAML is simpler than Python does not survive contact with this
problem, because the difficulty is not the declaration but type control, target
selection, and lineage granularity — and YAML helps with none of the three.

**The genuine argument for Meltano** is that it was taught in module 2.6: a
working reference exists, and familiarity reduces risk for engineers less
comfortable in Python. That is a legitimate basis, and it is worth being precise
about what it buys and costs. It buys a known path for the EL step alone. It
costs per-table lineage in the asset graph — the capability Dagster was chosen
for — and it moves the zip-code correctness risk into a plugin combination
nobody on the team has debugged. The decision rule adopted here: **dlt**, unless
the implementing engineers would materially rather follow a pattern they have
already run end to end — in which case Meltano is defensible, saves nothing on
the Kaggle step, and warrants an afternoon budgeted for `target-bigquery`
selection and a paragraph in the report conceding the coarser lineage.

The pipeline is wrapped as a Dagster asset (see §8) rather than invoked by a
standalone script, so it participates in the lineage graph.

---

## 5. Warehouse design

The dimensional model is not yet checked in as a diagram. Regenerate it with
`python3 docs/diagrams/generate_diagrams.py`, which emits
`02-warehouse-dimensional-model` in both formats.

### 5.1 dbt layering

| Layer | Materialisation | Count | Purpose |
|---|---|---|---|
| `sources` | — | 9 | `olist_raw.*` with freshness tests |
| `staging/stg_*` | view | 9 | 1:1 with source. Rename, cast, deduplicate. All casting logic lives here and only here |
| `intermediate/int_*` | view | 4 | Cross-table business logic, independently testable |
| `marts/dim_*`, `fct_*` | table | 8 | The only layer analysts are expected to query |

Four layers rather than raw → marts, because when a number looks wrong there are
three places to look instead of one 200-line query. Staging guarantees every
downstream model sees one consistent typed view of each source. Intermediate holds
the genuinely hard logic where it can be tested on its own rather than buried
inside a mart.

**Models:**

```
staging/        stg_customers, stg_orders, stg_order_items, stg_order_payments,
                stg_order_reviews, stg_products, stg_sellers, stg_geolocation,
                stg_product_category_translation

intermediate/   int_geolocation_deduped, int_order_lifecycle,
                int_order_payment_totals, int_customer_order_history

marts/          dim_date, dim_customer, dim_product, dim_seller,
                fct_orders, fct_order_items, fct_payments, fct_reviews
```

### 5.2 Dimensional model

A **fact constellation**: four fact tables sharing four conformed dimensions.
`fct_order_items` is the atomic sales fact.

| Table | Grain | Rows |
|---|---|---|
| `fct_order_items` | one order line (`order_id` + `order_item_id`) | ~112,650 |
| `fct_orders` | one order — accumulating snapshot | ~99,441 |
| `fct_payments` | `order_id` + `payment_sequential` | ~103,886 |
| `fct_reviews` | one review | ~99,224 |
| `dim_customer` | `customer_unique_id` | ~96,096 |
| `dim_product` | `product_id` | ~32,951 |
| `dim_seller` | `seller_id` | ~3,095 |
| `dim_date` | one day, 2016-09-01 → 2018-12-31 | 852 |

**Why a constellation rather than a single star.** A single fact table forces a
grain that either discards line-item detail or fans payments out across items,
inflating revenue. Separate facts at their natural grains, joined through
conformed dimensions, is both correct and standard Kimball.

**Why `dim_customer` is keyed on `customer_unique_id`.** `customer_id` in the raw
data is a *per-order* surrogate. Keying the dimension on it produces ~99k customers
who each ordered exactly once, and the conclusion that Olist has no repeat
business. The true figure is ~96k unique customers with roughly 3% repeating. Any
RFM segmentation built on the wrong key is meaningless. This is the single most
consequential modelling decision in the project.

**Why `fct_payments` stays separate.** Payments are at order grain, items at line
grain. Joining them multiplies `payment_value` by the number of lines in the order.
Keeping them apart and reconciling in `int_order_payment_totals` is the correct
handling; the fan-out is documented as a known trap.

**Why there is no `dim_geography`.** Raw geolocation is ~1M rows across ~19k zip
prefixes. `int_geolocation_deduped` reduces it to one median lat/lng per prefix,
and those attributes are denormalised into `dim_customer` and `dim_seller` rather
than kept as an outrigger. BigQuery's columnar storage compresses the repeated
values to near-nothing, and denormalising removes a join from every geographic
query. On a row-store the trade-off would go the other way.

### 5.3 Physical design

| Table | Partition | Cluster |
|---|---|---|
| `fct_order_items` | `order_purchase_date` (DAY) | `product_key`, `seller_key` |
| `fct_orders` | `order_purchase_date` (DAY) | `customer_key`, `order_status` |
| `fct_payments` | — | `order_id` |
| `fct_reviews` | `review_creation_date` (DAY) | `order_id` |
| dimensions | — | primary key |

Rationale: every meaningful analytical query filters on a purchase date range and
groups by product, seller, or customer. See the caveat in §3 about scale.

---

## 6. Data cleaning allocation

**Governing rule:** clean at the earliest layer with enough context to decide
correctly, and never clean the same thing twice.

| Layer | Class of cleaning | Examples | Prohibited here |
|---|---|---|---|
| dlt | Type enforcement only | zip prefixes as `STRING`; timestamps parsed; contract `freeze` | Dropping rows, filling nulls, renaming — raw must stay faithful to source |
| staging | Structural, deterministic, single-table | snake_case renames (the source ships `product_name_lenght`); whitespace trim; city-name normalisation; dedupe repeated `review_id` keeping latest `review_answer_timestamp` | Cross-table logic, business rules |
| intermediate | Cross-table reconciliation and derivation | geolocation dedupe and out-of-Brazil coordinate removal; order lifecycle deltas; payment reconciliation; which `order_status` values count as revenue | Presentation shaping |
| marts | None — shaping only | Unknown-member handling in dimensions | Any cleaning. Cleaning in a mart means the logic is in the wrong layer and will be duplicated |
| tests (dbt, GX) | **Verify, never repair** | Assert the invariants the layers above claim to produce | Fixing anything. A test that repairs data hides the defect it found |
| notebooks | **None** | — | Everything. Cleaning here means each analyst gets different numbers |

That last row is the justification for the warehouse itself, and the answer to
"why not just hand people CSVs."

### Nulls: not-applicable versus broken

Distinguish these explicitly; conflating them either destroys real signal or
hides real breakage.

- `order_delivered_customer_date` is null for genuinely undelivered orders. Keep
  it. `delivery_days` is null and `is_late` is **null, not false**.
- `product_category_name` is null for ~610 products with no legitimate reason.
  `dim_product` maps it to an explicit `'unknown'` member, because a dimension
  attribute used for grouping should never be null.
- `review_comment_message` is frequently null and legitimately so. Keep it, and
  add a `has_comment` boolean for analysis.

---

## 7. Data quality strategy

Two tiers, doing two different jobs.

### Tier 1 — dbt tests (every build, gate the pipeline)

- `unique` + `not_null` on every primary key
- `relationships` on every foreign key into its dimension
- `accepted_values` on `order_status` and `payment_type`
- `dbt_utils.expression_is_true`: `price >= 0`, `freight_value >= 0`,
  `review_score between 1 and 5`
- `dbt_utils.unique_combination_of_columns` for the composite grains

Cheap, schema-level, and they fail the build.

### Tier 2 — Great Expectations (after marts, business invariants)

1. **Payment reconciliation.** Per order, `sum(payment_value)` versus
   `sum(price + freight_value)` within tolerance. Expect ≥99% to pass; investigate
   and *document* the residual rather than suppressing it — vouchers and partial
   payments produce genuine mismatches.
2. **Date monotonicity.** `purchase ≤ approved ≤ carrier handover ≤ delivered`.
   Known violations exist in the source; quantify them.
3. **Row-count stability.** Fact tables within an expected band, catching a
   partial load.
4. **Referential completeness.** Every `fct_order_items.customer_key` resolves.
5. **Distribution checks.** Mean `review_score` plausible; monthly order volume
   non-zero across the window.

**Why both.** dbt tests are fast and structural — they belong inline in every
build. GX handles cross-table and statistical assertions dbt expresses awkwardly,
and it emits **Data Docs**, a browsable HTML quality report. That artifact is
concrete evidence for the documentation criterion in a way that `dbt test` console
output is not.

---

## 8. Orchestration

Dagster software-defined assets. One graph, end to end:

```
kaggle_dataset          (asset: download at pinned version → GCS)
  └→ gcs_raw_files      (asset: 9 CSVs under ingest_date=…)
       └→ dlt assets    (9, one per source table → olist_raw)
            └→ dbt staging assets      (9)   ┐
                 └→ dbt intermediate    (4)  │ auto-generated from the
                      └→ dbt marts      (8)  ┘ dbt manifest via @dbt_assets
                           └→ gx_validation   (asset check)
                                └→ analytics_extract  (optional, §11)
```

- `@dbt_assets` generates one Dagster asset per dbt model from the manifest, so
  lineage stays truthful without a hand-maintained parallel DAG.
- `dagster-dlt`'s `DagsterDltResource` wraps the ingestion pipeline.
- GX results surface as Dagster **asset checks**, so a quality failure shows up
  against the asset that produced it rather than as an unrelated task failure.
- Schedule: daily, 08:00 SGT.

### Where Dagster runs

**Dagster is not hosted.** The `dagster-daemon` is what makes schedules fire, so
an unattended schedule requires an always-on process — and that is the entire
cost of hosting it. GCP's always-free `e2-micro` has 1 GB of RAM, which the
webserver, daemon, dbt, and dlt will exhaust; anything larger is real money and an
extra machine to maintain during the final week. Cloud Run is request-scoped and
would drag in Cloud SQL for run storage. Dagster+ is a commercial product adding
an account and agent setup for no grading benefit. The brief's §6 is optional and
names GitHub Actions as acceptable.

`orchestration/definitions.py` remains the single source of truth, executed two
ways:

| Context | Command | Purpose |
|---|---|---|
| Development and demo | `dagster dev` | Webserver on `localhost:3000` plus daemon. Materialise assets, inspect lineage, demo live. The asset graph screenshot is the strongest visual available for the Technical Overview slide |
| Scheduled execution | GitHub Actions cron → `dagster.materialize(...)` in-process | A genuinely running daily schedule, free, with no infrastructure and no daemon |

The daemon *is* the scheduler; once GitHub Actions holds that role, production
does not need it. The service-account JSON and Kaggle credentials live in GitHub
Actions secrets, never in the repository.

Report this as the deliberate simplification it is: the production path would be
Dagster+ or a container on GKE.

### Reading pipeline reports without a daemon

A GitHub Actions runner is destroyed after each job, and Dagster's own
documentation is explicit: with `DAGSTER_HOME` unset, the instance uses a
temporary directory *cleared on process exit*. Run history and materialization
records therefore do not survive a scheduled run. This must be designed for
rather than discovered.

Four distinct artifacts are conflated under "reports", and only one needs Dagster:

| Question | Answer lives in | Survives an ephemeral run |
|---|---|---|
| Did the pipeline run and pass? | Actions run log and status badge | Yes |
| What did dbt do — models, timings, tests? | `target/run_results.json`, dbt docs | Only if exported |
| Did quality checks pass, and what failed? | GX Data Docs HTML | Only if exported |
| Which assets materialised when, historically? | Dagster instance storage | **No** |

**Publish the static reports (required).** GX Data Docs and `dbt docs`
are static HTML. The workflow uploads them as artifacts with `if: always()` — they
matter most when a run fails — and deploys them to **GitHub Pages**, yielding
permanent URLs citable from the report and the deck. A published quality report is
materially stronger evidence for the documentation criterion than a screenshot.

**Run history is local, and deliberately stops there.** Set `DAGSTER_HOME` and
Dagster's default storage takes over: SQLite under `history/` for run and event
records, `storage/` for compute logs, both gitignored. `orchestration/dagster.yaml`
therefore carries no `storage:` block at all — omitting it *is* the choice. That
costs nothing, provisions nothing, and covers the two places history is actually
read: `dagster dev` on a laptop during development and the demo, and the console
in `deploy/` (below).

**Shared history was considered and rejected.** Dagster's only alternatives to
SQLite are Postgres and MySQL, so one instance readable from both CI and every
laptop means hosting a database — a free-tier Supabase project was the obvious
candidate. Against it: a second stateful service to own, credentials to
distribute to six people, and a free tier that pauses on inactivity so a
scheduled run needs a retry to wake it — all to produce evidence the brief does
not ask for and that the published reports already cover more durably. §3 rejects Postgres
as the warehouse; re-admitting it to hold instance metadata is not a better
trade.

**The consequence, stated in the report rather than discovered in Q&A:** run
history from scheduled runs is ephemeral by design, because the orchestrator is
not hosted and the runner is destroyed after each job. The durable evidence is
the published Data Docs and dbt docs. A deliberately chosen and documented
limitation reads as engineering judgement; the same limitation surfaced by a
marker's question does not.

**Stated honestly in the report:** the dataset is a static dump ending October
2018, so a daily schedule performs no new work. The pipeline is *built* for
incremental arrival — idempotent replace loads, a date-partitioned raw zone,
incremental-ready marts — and demonstrating that on a static dataset is a
deliberate simplification. Presenting a daily refresh as if it did real work
would not survive Q&A.

### Docker Compose considered

Compose is a packaging format, not a host. It describes how processes are
assembled; it does not answer where they stay running, which is the entire
question above. The daemon still needs an always-on machine, and that machine is
still the whole cost.

It also makes the hosting arithmetic worse rather than better. Dagster's
reference Compose deployment is four long-lived services — Postgres, a user-code
gRPC container, the webserver, and the daemon — plus two Dockerfiles, a
`workspace.yaml`, and `/var/run/docker.sock` mounted into the webserver and
daemon. With `DAGSTER_CURRENT_IMAGE` set, the run launcher starts a *further*
container for each run.

| Service | Approximate resident memory |
|---|---|
| Postgres | ~150 MB |
| user-code gRPC (dbt, dlt, pandas imported) | ~300 MB |
| webserver | ~250 MB |
| daemon | ~200 MB |
| per-run container, during a materialisation | ~400 MB |

Roughly 1.5–2.5 GB at peak. The always-free `e2-micro` has 1 GB, so a topology
adopted to make hosting easier in fact rules out the only free host. A
comfortable `e2-medium` is about $25–35 per month — real money, and a machine to
patch during the final week.

**It does not recover shared history either.** Whatever Compose starts is local
to the machine that ran `docker compose up`, and the GitHub Actions runner
cannot reach it, so scheduled-run history is no more durable for its existence.
Compose is orthogonal to that question rather than an answer to it — and the
question was already settled above, in the negative.

**The reproducibility case is weaker than it looks.** Containers earn their cost
where there are system-level dependencies — compilers, geospatial libraries, a
JVM. This stack is pure pip (dlt, dbt-bigquery, Dagster, pandas) with the heavy
compute pushed into BigQuery, so a pinned `pyproject.toml` and a lockfile
recover most of the same guarantee. Against that: Docker Desktop installs across
six machines, ARM-versus-x86 wheel surprises on M-series Macs, mounting the
service-account JSON without leaking it, and a `dbt run` loop that is slower in
a container than in a virtualenv — a cost Lane A2 pays daily through week 2,
while Lane C, which installs nothing by design (§15), gains nothing.

**What is worth building, in week 3.** A deliberately slim **two-service**
Compose file — webserver and daemon only — sharing a bind-mounted `DAGSTER_HOME`
so the UI reads the same local SQLite history `dagster dev` writes. No database
service, no gRPC code-location container, no `docker.sock`.

It buys one thing, and the file should not claim more: §8's closing assertion
that the production path would be Dagster+ or a container on GKE becomes
something a marker can read — cheap evidence for the architecture criterion. The
image carries `dagster` and `dagster-webserver` only, not dbt/dlt/pandas, so it
loads no code location; it is a history console and a topology demonstration,
not a way to run the pipeline.

Scheduled for week 3 alongside the docs publishing, never week 1, where it would
compete with the critical path.

---

## 9. Analysis layer

Connection via SQLAlchemy with the `sqlalchemy-bigquery` dialect, reading
`olist_marts` only. Notebooks never touch `raw` or `staging`.

| Notebook | Content |
|---|---|
| `01_data_profiling.ipynb` | Row counts, null profiles, grain verification, the quality findings from §7 |
| `02_sales_trends.ipynb` | Monthly revenue, order volume, average order value, seasonality |
| `03_product_and_seller_performance.ipynb` | Top categories, category revenue concentration, seller distribution, freight as a share of revenue |
| `04_customer_segmentation.ipynb` | RFM segmentation on `customer_unique_id`, repeat-purchase behaviour, geographic concentration |

**The analytical trap to avoid.** The dataset ends part-way through October 2018,
so the final months show an apparent collapse in revenue that is an artifact of
truncation, not a business event. Any monthly trend chart must either exclude the
incomplete tail or annotate it. Presenting that decline to an executive audience
as a finding is the most likely way to lose credibility in Q&A.

Expect Black Friday 2017 (late November) to be the clearest genuine seasonal
signal in the series.

---

## 10. Repository layout

Annotated with the owning lane from §15.

```
olist-data-platform/
├── README.md                          # setup, run instructions, report URLs   C2
├── pyproject.toml
├── .gitignore                         # *.json, .env, target/, uncommitted/    A1
├── .env.example                       # documents required vars, no values     A1
├── ingestion/                                                               # A1
│   ├── kaggle_to_gcs.py               # kagglehub → unzip → GCS  (§4)
│   ├── olist_source.py                # dlt source, explicit column hints
│   └── gcs_to_bigquery.py             # dlt pipeline → olist_raw
├── transform/                         # dbt project                          # A2
│   ├── dbt_project.yml
│   ├── profiles.yml                   # location: US, dev_<name> targets
│   ├── models/
│   │   ├── staging/                   # 9 models + schema.yml                  C1
│   │   ├── intermediate/              # 4 models + schema.yml                  C1
│   │   └── marts/                     # 8 models + schema.yml                  C1
│   ├── macros/
│   ├── seeds/                         # product_category_name_translation
│   └── tests/                         # singular tests                         B1
├── quality/                                                                 # B1
│   └── great_expectations/
│       └── uncommitted/data_docs/     # generated; gitignored, published to Pages
├── orchestration/                                                           # A1
│   ├── definitions.py                 # single source of truth for the graph
│   ├── assets.py                      # kaggle→gcs, dlt, @dbt_assets, gx check
│   ├── resources.py
│   ├── schedules.py                   # declares intent; GH Actions fires it
│   ├── dagster.yaml                   # local SQLite instance, no DB (§8)
│   └── run_all.py                     # materialize() entrypoint for CI
├── deploy/                            # stretch, week 3 only  (§8)            # A1
│   ├── docker-compose.yml             # webserver + daemon, bind-mounted home
│   └── Dockerfile                     # dagster-webserver only
├── notebooks/                         # one per person, nbstripout installed    B2
├── docs/
│   ├── architecture-design.md         # this file
│   └── diagrams/                      # draw.io + Excalidraw, generated
└── .github/
    └── workflows/
        └── pipeline.yml               # cron → run_all.py → artifacts → Pages  A1
```

**`.gitignore` must exist in the first commit**, covering `*.json`, `.env`,
`transform/target/`, and `great_expectations/uncommitted/`. A service-account key
committed to history is laborious to expunge and is precisely the kind of lapse
the code-quality criterion penalises. All credentials — `GCP_SA_KEY`,
`GCP_PROJECT`, and `KAGGLE_API_TOKEN` — live in GitHub Actions secrets and in
each developer's local `.env`. Dagster contributes none: it holds no database
credentials to leak.

The brief requires a **single main branch**. Read as a description of the repo's
final state — all work on main, nothing stranded on side branches — rather than a
prohibition on pull requests. With six contributors, short-lived branches merged
via PR are safer and add code review, which serves the code-quality criterion. If
the requirement is meant literally, fall back to small frequent commits and a
pinned issue tracking who is editing what.

Two conventions that prevent most collisions:

- Each engineer targets a personal BigQuery dataset via a dbt target
  (`dbt run --target dev_<name>`), so concurrent work never collides in the shared
  `olist_marts`.
- **`nbstripout` is installed before anyone opens a notebook**
  (`pip install nbstripout && nbstripout --install`). Notebook JSON diffs on every
  cell execution and embeds outputs; without stripping, six people editing
  notebooks in one repo produces continuous merge conflicts. One notebook per
  person — never a shared one.

---

## 11. Out of scope, and stretch goals

**A dashboard is a conditional stretch goal, not a component.** No dashboard is a
required deliverable, and none advances the four focus criteria directly.

If one is built, use **Looker Studio**, not Streamlit. Looker Studio connects to
BigQuery natively, requires no code, costs nothing, and can be owned end to end by
a non-engineering team member (§15) while the engineers stay on the critical path.
Streamlit's real cost is engineering hours diverted from the pipeline; Looker
Studio's is close to zero of them.

Streamlit remains defensible only if a team member specifically wants the
engineering practice and the pipeline is already complete and green. In that case
it must read a **pre-aggregated Parquet or DuckDB extract** materialised by a
Dagster asset — never live BigQuery, which turns every widget interaction into a
query job and puts a network dependency in the middle of a timed presentation.
Three or four pages maximum.

Either way, it is framed in the deck as evidence that the marts are consumable
without SQL.

The second stretch goal is the two-service Compose file in `deploy/` (§8), which
turns the claim that this would run as a container in production into something
a marker can read. It depends on nothing else — the Dagster instance it reads is
local SQLite — and is worth roughly two hours in week 3, or none at all.

Persistent *shared* run history is not a stretch goal; it is out of scope, and
§8 gives the reasoning.

Also out of scope: streaming ingestion, ML models, reverse ETL, and dbt snapshots
(the source has no change history to capture).

---

## 12. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| GCP billing or credentials fail near the deadline | Blocks everything | Service-account JSON out of git via `.env`; keep a DuckDB target in `profiles.yml` as a fallback so dbt models still build and the analysis still runs |
| Truncated Oct 2018 tail read as a revenue collapse | Credibility loss in Q&A | Annotate or exclude the incomplete period in every time-series chart (§9) |
| `customer_id` used instead of `customer_unique_id` | Customer analysis becomes meaningless | Enforced in `dim_customer`; a dbt test asserts the dimension's row count is below the order count |
| Payment fan-out inflates revenue | Wrong headline numbers | Separate fact tables; GX reconciliation test (§7) |
| Scheduling a static dataset reads as cargo-culting | Architecture criterion | Address it explicitly in the report and the deck (§8) |
| Scope creep into dashboards or streaming | Missed deadline | §11 |
| Service-account key or Kaggle token committed to git | Credential leak; code-quality penalty | `.gitignore` in the first commit; all secrets in GitHub Actions secrets and local `.env` (§10) |
| Dagster run history lost because the runner is ephemeral | No observability over scheduled runs | Accepted and documented, not mitigated: the durable evidence is Data Docs and dbt docs published to Pages; local history persists under `DAGSTER_HOME` for development (§8) |
| Artifacts not uploaded on a failed run | Cannot debug a failed nightly | `if: always()` on the upload step (§8) |
| Team collisions in a shared dataset on one main branch | Broken builds | Per-developer dbt targets; `nbstripout` for notebooks (§10) |
| Non-engineering members idle while waiting on the pipeline | Wasted capacity, weak documentation | Lane C starts day 1 from this document, not from working code (§15) |
| Technical scope inflated because the team has six people | Missed deadline | The pipeline spine takes 2–3 engineers; surplus capacity goes to documentation and the deck, not more components (§15) |

---

## 13. Open questions

1. Whether an external enrichment dataset (e.g. Brazilian public holidays, to
   explain seasonality) is worth the ingestion cost. The brief permits it.
2. GCP project id and naming convention for the per-developer dev datasets.

## 14. Resolved decisions

**GCP location: `US` multi-region.** Fixed for every dataset and for the GCS raw
bucket. Rationale: the dataset is public and anonymised, so no data-residency
constraint applies and a São Paulo locality argument would be cosmetic; the `US`
multi-region is BigQuery's lowest pricing tier and receives features first; and
query latency is irrelevant because compute runs in BigQuery and only aggregated
result sets cross the network.

Two consequences the team must respect:

- **A BigQuery load job requires the GCS bucket and destination dataset to be in
  compatible locations.** A bucket outside the US with a `US` dataset fails the
  load with an error that appears to blame the bucket. Create the bucket with
  `--location=US`.
- **Dataset location is immutable.** Changing it means recreating and reloading.
  `location: US` must be set explicitly in `profiles.yml`, in the dlt destination
  config, and via `--location=US` on any `bq` CLI call, so that nothing is ever
  created from a console default on a different day.

---

## 15. Team and delivery model

Six members with mixed engineering backgrounds. The constraint that matters:
**the pipeline spine cannot absorb six engineers.** dlt → dbt → Dagster is a
critical path that two or three people build faster than six. Surplus capacity
goes to the graded work that engineering teams habitually under-serve —
documentation and the executive presentation account for two of the eight brief
sections and one of the four focus criteria.

### Lanes

| Lane | Members | Owns | Unblocked from |
|---|---|---|---|
| **A — Platform** | 2 engineers | **A1** Kaggle → GCS → dlt, Dagster assets, resources, schedule. **A2** the dbt project: 9 staging, 4 intermediate, 8 mart models, physical design | Day 1 |
| **B — Quality & Analysis** | 1 engineer, 1 analyst | **B1** dbt tests and GX suites (§7). **B2** the four notebooks (§9) | B1 day 1 as prose; B2 once staging exists |
| **C — Documentation** | 2 non-engineers | **C1** `description:` for every model and column in `schema.yml`, the data dictionary, the published `dbt docs` site. **C2** the report (§7 of the brief), the deck, exec framing, risk narrative | Day 1, from this document |

All six present, per the brief's guidance.

### Why C1 is real work, not make-work

dbt renders `schema.yml` descriptions into the searchable catalog and lineage
graph produced by `dbt docs generate`. Writing them requires domain understanding
and YAML — no Python, no SQL, no dbt execution — and they can be drafted from §5
of this document before a single model exists. Engineering teams routinely ship
empty descriptions and lose marks for it. Assigning the work to someone with time
to do it properly converts a predictable weakness into evidence for the
documentation criterion.

### Access model: non-engineers install nothing

One shared GCP project. Engineers hold the service-account key. Everyone else is
granted **`BigQuery Data Viewer` + `BigQuery Job User`** on their own Google
account and works in the **BigQuery web console** — no `gcloud`, no Python, no dbt,
no credentials file to leak. They can query `olist_marts` directly the day it
exists.

This is the architecture's central claim made literal: the marts layer is what
makes a non-engineer self-sufficient against the warehouse. It is also the
strongest available answer to "what business value did this deliver."

### Sequencing so nobody idles in week one

| | A1 | A2 | B1 | B2 | C1 | C2 |
|---|---|---|---|---|---|---|
| **Week 1** | GCS + dlt load | staging models | expectations as prose | profile raw/staging | descriptions from §5 | report skeleton, exec narrative |
| **Week 2** | Dagster assets | intermediate + marts | dbt tests wired | sales + product notebooks | descriptions vs real models | draft report |
| **Week 3** | schedule, docs publish | physical design, tuning | GX suites + asset checks | RFM segmentation | `dbt docs` site published | deck, charts, rehearsal |
