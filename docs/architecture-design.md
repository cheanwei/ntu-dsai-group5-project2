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
                              dbt + dbt-expectations    SQLAlchemy + pandas
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
| Quality | **dbt tests + dbt-expectations** | A second assertion framework | Two tiers, one runner — see §7. |
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

- **A fixed schema, not an inferred one.** All 52 columns across the nine tables
  are declared in `ingestion/config.yml` with the type they must land as, and
  both halves of the load derive from that one declaration — the pandas parse
  and the dlt column hints — so the two cannot drift apart. Three traps motivate
  this. `customer_zip_code_prefix` and `seller_zip_code_prefix` carry leading
  zeros and must be `STRING`; inferred as `INT64` they are silently corrupted.
  The `order_*_timestamp` and `*_date` columns need explicit `TIMESTAMP` typing.
  And `review_comment_title` is ~88% null, so a slice carrying no comment gives
  inference nothing to work with and the column is dropped — which makes the raw
  table's shape depend on which rows happened to arrive. Declaring every column
  removes the class of problem rather than the known instances.
- **Schema contract set to `freeze`.** Unexpected columns or type drift fail the
  load rather than silently reshaping the warehouse. Because the schema is
  declared rather than discovered, this holds on the *first* load: there is no
  run in which dlt is still learning the shape. `tables` stays `evolve` and must
  — dlt evaluates the contract against the stored schema, so on a fresh dataset
  all nine tables read as new and `freeze` would block the pipeline from ever
  creating them. Which tables exist is gated upstream instead, by the source
  yielding one resource per configured entry and nothing else.
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
| dlt | Type enforcement only | all 52 columns declared, not inferred; contract `freeze` | Dropping rows, filling nulls, renaming — raw must stay faithful to source |
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

### Tier 2 — dbt-expectations (after marts, business invariants)

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

Tier 2 lives in `transform/`: generic tests beside the column they guard in
`models/marts/schema.yml`, and singular tests in `tests/` for the assertions
that span two facts, such as the payment reconciliation.

**Why one tool and not two.** The tiers differ in what they assert, not in what
runs them. `dbt_expectations` supplies the Great Expectations-style macros —
distributions, row-count bands, column-pair comparisons — that plain dbt
expresses awkwardly, so tier 2 needs no second framework, no second datasource
configuration, and no second place to look when something fails. Both tiers run
in one `dbt build`, which is already a node in the asset graph, so a tier-2
failure stops the marts the same way a tier-1 failure does.

It also gets the attribution this section wanted for free: **dagster-dbt models
every dbt test as an asset check**, so a failing payment reconciliation lands on
`fct_orders` rather than on a nameless task. That needed hand-written
`@asset_check` wrappers under the previous design; here it is the default, and
`enable_source_tests_as_checks` extends it to the raw tables.

**What this gives up.** Great Expectations emits **Data Docs**, a browsable HTML
quality report, and nothing here replaces that artifact. Test *definitions* are
published in the dbt docs site; pass/fail shows in the Dagster UI as asset
checks and in the Actions run. If a standalone HTML quality report is wanted
later, generate it from `run_results.json` rather than reinstating a second
assertion framework.

---

## 8. Orchestration

Dagster software-defined assets. One graph, end to end:

```
kaggle_dataset          (asset: download at pinned version → GCS)   ┐ daily_refresh
  └→ gcs_raw_files      (asset: 9 CSVs under ingest_date=…)         │ schedule, cron
       └→ dlt assets    (9, one per source table → olist_raw)       ┘ 08:00 SGT
            └→ dbt staging assets      (9)   ┐ @dbt_assets, from the manifest
                 └→ dbt intermediate    (4)  │ automation_conditions sensor,
                      └→ dbt marts      (8)  ┘ eager on the load above
                                └→ analytics_extract  (optional, §11)
```

- `@dbt_assets` generates one Dagster asset per dbt model from the manifest, so
  lineage stays truthful without a hand-maintained parallel DAG.
- `dagster-dlt`'s `DagsterDltResource` wraps the ingestion pipeline.
- GX results surface as Dagster **asset checks**, so a quality failure shows up
  against the asset that produced it rather than as an unrelated task failure.

**Two triggers, and the boundary between them is the point.** Nothing external
announces a change to the Kaggle dump, so ingestion is *pulled* on a cron:
`daily_refresh` materialises the ingestion group — Kaggle → GCS → `olist_raw` —
at 08:00 SGT. The load finishing, by contrast, **is** an event this system
emits. So the dbt layer is not scheduled at all. Every model carries
`AutomationCondition.eager()` (`orchestration/assets.py`), and the
`automation_conditions` sensor (`orchestration/sensors.py`) requests a build
when the raw tables it reads have actually been reloaded.

| Name | Kind | Fires | Materialises |
|---|---|---|---|
| `daily_refresh` | schedule | 08:00 SGT | `kaggle_dataset` → `gcs_raw_files` → the 9 raw tables |
| `automation_conditions` | sensor | when that load lands | the 21 dbt models, per model |

Both are held by the daemon on `dagster-vm` (below), and both default to
`RUNNING` so that a fresh `DAGSTER_HOME` does not silently start with the
pipeline switched off.

The reason to split them rather than run one cron over `AssetSelection.all()`:
under a single cron, dbt runs whether or not the load succeeded. An ingestion
failure at 08:00 would rebuild the marts on yesterday's raw tables and report a
green dbt run beside a red one. Under a condition, the trigger is the load
completing, so a failed ingestion produces no dbt run at all and the marts hold
their last good state. The dependency edge does the work that an `if` in a task
DAG would otherwise have to.

This is also the honest answer to the objection in §12 that scheduling a static
dataset is cargo-culting. The cron over a fixed dump genuinely performs no new
work; the *event-driven* half is the part that would be identical against a
partner dropping a file or an hourly export, and it is structural rather than
asserted.

### Where Dagster runs

**Dagster is hosted, on one always-free machine.** The `dagster-daemon` is what
makes a schedule fire and a sensor tick, so both triggers above need a process
that is alive at 08:00 and still alive when a load finishes. That process is a
**two-service Docker Compose deployment on a GCP `e2-micro`**
(`orchestration/deploy/`): webserver and daemon, each loading the code location
in-process, sharing a bind-mounted `DAGSTER_HOME` backed by SQLite. Roughly
450–550 MB resident, on a 1 GB machine with 2 GB of swap behind it.

The fit is bought, not lucky. Dagster's reference Compose deployment is five
moving parts and 1.5–2.5 GB at peak, which rules out the only free host;
dropping three of them is what makes an `e2-micro` viable, and the trade is set
out below. The alternatives were weighed and rejected on the same arithmetic:
Cloud Run is request-scoped and would drag in Cloud SQL for run storage, and
Dagster+ is a commercial product adding an account and agent setup for no
grading benefit.

`orchestration/definitions.py` remains the single source of truth, executed
three ways:

| Context | Command | Purpose |
|---|---|---|
| Development and demo | `dagster dev` | Webserver on `localhost:3000` plus daemon. Materialise assets, inspect lineage, demo live. The asset graph screenshot is the strongest visual available for the Technical Overview slide |
| Production | webserver + daemon on `dagster-vm` (`docker-compose.vm.yml`) | Holds both triggers. Run history is durable here, and the UI is reachable over IAP |
| One-shot, and CI | `run_all.py` | Materialises the graph in-process, ignoring both triggers. The path a developer and the reports workflow take |

The VM has a public IP and is not reachable on it: the address carries egress to
`kaggle.com`, while ingress on 22 and 3002 is allowed only from IAP's
`35.235.240.0/20`. The service-account JSON and Kaggle credentials live in
GitHub Actions secrets and are delivered to the VM by the deploy workflow —
never in the repository, never baked into the image.

**`.github/workflows/pipeline.yml` holds no cron.** It is a reports-only
workflow, run on demand to publish the dbt docs site to Pages. Two schedulers
materialising the same assets at the same instant would race on the same
BigQuery tables, and a race whose cause is a workflow trigger is one nobody
would attribute correctly; `tests/test_orchestration_definitions.py` asserts the
cron stays absent.

State the limit that remains rather than waiting for it in Q&A: this is one
machine, unreplicated, with the daemon both launching and executing every run.
A container restart kills whatever is materialising. That is acceptable for a
project deployment and is the first thing to revisit if this were ever run for
real, where the answer is Dagster+ or a container on GKE.

### Run history and published reports

Four distinct artifacts are conflated under "reports", and they do not live in
the same place or survive the same failures:

| Question | Answer lives in | Durable |
|---|---|---|
| Did the pipeline run and pass? | Dagster run log on `dagster-vm`; Actions log for the reports workflow | Yes |
| What did dbt do — models, timings, tests? | `target/run_results.json`, dbt docs | Only if exported |
| Did quality checks pass, and what failed? | Dagster asset checks; `run_results.json` | Only if exported |
| Which assets materialised when, historically? | Dagster instance storage on the VM | Yes — SQLite under `DAGSTER_HOME`, across redeploys and reboots |

The last row is the one the hosting decision buys. A GitHub Actions runner is
destroyed after each job, and with `DAGSTER_HOME` unset Dagster uses a temporary
directory *cleared on process exit* — so under the Actions-as-scheduler topology
no materialisation record survived the run that wrote it. The VM's bind-mounted
`DAGSTER_HOME` is the one thing that arrangement could not provide, and the
sensor needs it: automation conditions are evaluated against materialisation
history and the sensor's own cursor, both of which live in instance storage. An
instance cleared on process exit has neither, so there is no "since the last
load" for a condition to be true of.

**Publish the static reports (required).** The `dbt docs` site is static HTML. The workflow uploads them as artifacts with `if: always()` — they
matter most when a run fails — and deploys them to **GitHub Pages**, yielding
permanent URLs citable from the report and the deck. A published quality report is
materially stronger evidence for the documentation criterion than a screenshot.

**Run history is durable but not shared, and deliberately stops there.** Set
`DAGSTER_HOME` and Dagster's default storage takes over: SQLite under
`history/` for run and event records, `storage/` for compute logs, both
gitignored. `orchestration/dagster.yaml` therefore carries no `storage:` block
at all — omitting it *is* the choice. That costs nothing, provisions nothing,
and covers the two places history is actually read: the VM's UI over IAP, and
`dagster dev` on a laptop during development and the demo. Those are two
separate instances with separate histories, which is the trap to know about
before wondering why a laptop shows no scheduled runs.

**Shared history was considered and rejected.** Dagster's only alternatives to
SQLite are Postgres and MySQL, so one instance readable from the VM and every
laptop means hosting a database — a free-tier Supabase project was the obvious
candidate. Against it: a second stateful service to own, credentials to
distribute to six people, and a free tier that pauses on inactivity so a
scheduled run needs a retry to wake it — all to produce evidence the brief does
not ask for and that the published reports already cover more durably. §3 rejects Postgres
as the warehouse; re-admitting it to hold instance metadata is not a better
trade.

**The consequence, stated in the report rather than discovered in Q&A:**
scheduled-run history is durable, and it is durable in exactly one place. It is
not replicated, not backed up, and not readable from a laptop or a CI runner —
so the evidence cited in the report and the deck is the published dbt docs,
which have permanent URLs, and the run history is what the demo shows live.

**Stated honestly in the report:** the dataset is a static dump ending October
2018, so a daily cron performs no new work. The pipeline is *built* for
incremental arrival — idempotent replace loads, a date-partitioned raw zone,
incremental-ready marts, and a transform layer triggered by arrival rather than
by the clock — and demonstrating that on a static dataset is a deliberate
simplification. Presenting a daily refresh as if it did real work would not
survive Q&A; presenting the *mechanism* as the deliverable it is, does.

### Why not Dagster's reference Compose deployment

Compose is a packaging format, not a host. It describes how processes are
assembled; it does not answer where they stay running, and the always-on machine
is the whole cost either way — which is why the choice above is a VM rather than
a `docker-compose.yml`.

Adopting Dagster's *reference* topology on top of that would have made the
arithmetic worse rather than better. It is four long-lived services — Postgres, a user-code
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

**It does not make history shared either.** Whatever Compose starts is local to
the machine that ran `docker compose up`; a Postgres service in the same file is
reachable from that host and nowhere else. The durability the deployment
delivers comes from the bind-mounted `DAGSTER_HOME` surviving the container, not
from the database engine underneath it — which is why the slim version gives up
Postgres without giving up the row that mattered in the table above.

**The reproducibility case is weaker than it looks.** Containers earn their cost
where there are system-level dependencies — compilers, geospatial libraries, a
JVM. This stack is pure pip (dlt, dbt-bigquery, Dagster, pandas) with the heavy
compute pushed into BigQuery, so a pinned `pyproject.toml` and a lockfile
recover most of the same guarantee. Against that: Docker Desktop installs across
six machines, ARM-versus-x86 wheel surprises on M-series Macs, mounting the
service-account JSON without leaking it, and a `dbt run` loop that is slower in
a container than in a virtualenv — a cost Lane A2 pays daily through week 2,
while Lane C, which installs nothing by design (§15), gains nothing.

**What was built instead.** A deliberately slim **two-service** Compose file —
webserver and daemon, each loading the code location itself — sharing a
bind-mounted `DAGSTER_HOME`, so the UI reads the same SQLite history the daemon
writes. No database service, no gRPC code-location container, no `docker.sock`:
two of the reference deployment's five moving parts, and the reason 450–550 MB
fits where 1.5–2.5 GB does not. The cost of dropping the code server is that the
daemon both launches and hosts every run — Dagster's default run coordinator is
the queued one, so even a run submitted in the UI is executed by the daemon
beside its own copy of the code — which makes the daemon a single point of
failure and a container restart fatal to whatever is running.

It also concentrates a second responsibility there. The daemon does not merely
fire the 08:00 cron; it evaluates the `automation_conditions` sensor, which is
the only thing that builds the dbt layer now that the schedule covers ingestion
alone. Stopping that service to reclaim memory does not delay the daily run, it
leaves the marts unbuilt indefinitely while ingestion keeps reporting green.

The revision from the *console* originally planned here was made deliberately,
and it changes what the file buys. An image carrying only `dagster` and
`dagster-webserver` cannot load a code location, so it could show history but
never materialise an asset — and `dagster-webserver` will not even start
without a target unless told the empty workspace is intended. Putting the
project's own dependencies and code in the image makes it *run* the pipeline
instead: a run launched from the UI or ticked by the daemon does what
`dagster dev` and the CI workflow do, with the service-account key bind-mounted
rather than baked in.

The cost is honest and bounded: the image is ~1.5 GB because it carries dbt,
dlt, GX and pandas, and a run against it spends real Kaggle bandwidth and real
BigQuery. §8's closing assertion — that the production path would be Dagster+
or a container on GKE — stops being an assertion either way; it is now backed by
a container that has actually run the graph rather than one that only draws it.

Built in week 3 alongside the docs publishing, never week 1, where it would have
competed with the critical path. `orchestration/deploy/provision_vm.sh` stands
the machine up; `.github/workflows/deploy-dagster.yml` builds the image, pushes
it to Artifact Registry pinned to a commit SHA, and restarts the two services.

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
│   ├── olist_source.py                # dlt source, fixed schema from config.yml
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
│   └── tests/                         # singular tests — tier 2 (§7)           B1
├── orchestration/                                                           # A1
│   ├── definitions.py                 # single source of truth for the graph
│   ├── assets.py                      # kaggle→gcs, dlt, @dbt_assets
│   ├── resources.py
│   ├── schedules.py                   # daily_refresh: cron, ingestion only (§8)
│   ├── sensors.py                     # automation_conditions: dbt on the load (§8)
│   ├── dagster.yaml                   # local SQLite instance, no DB (§8)
│   ├── run_all.py                     # the single entrypoint: laptop and CI
│   └── deploy/                        # the hosted daemon on dagster-vm (§8)
│       ├── docker-compose.yml         # webserver + daemon, bind-mounted home
│       ├── docker-compose.vm.yml      # the hosted variant: pulls a pinned SHA
│       ├── provision_vm.sh            # the machine, the firewall, IAP ingress
│       └── Dockerfile                 # project deps; runs the pipeline
├── notebooks/                         # one per person, nbstripout installed    B2
├── docs/
│   ├── architecture-design.md         # this file
│   └── diagrams/                      # draw.io + Excalidraw, generated
└── .github/
    └── workflows/
        ├── pipeline.yml               # on demand → run_all.py → Pages         A1
        └── deploy-dagster.yml         # build image → deploy to the VM          A1
```

**`.gitignore` must exist in the first commit**, covering `*.json`, `.env`,
`transform/target/`, and `transform/dbt_packages/`. A service-account key
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

The second stretch goal, the Compose deployment in `orchestration/deploy/`
(§8), was built. It is no longer a claim that this *would* run as a container in
production: the daemon on `dagster-vm` holds both triggers, and the marts are
built by a container that has actually run the graph rather than one that only
draws it.

Persistent *shared* run history is still not a stretch goal; it is out of scope,
and §8 gives the reasoning. Durable history and *shared* history are different
things, and only the first was in reach.

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
| Scheduling a static dataset reads as cargo-culting | Architecture criterion | Partly designed out rather than argued away: only ingestion is on a cron, and the dbt layer is triggered by the load landing, which is the mechanism a real arrival would use. The remaining cron is addressed explicitly in the report and the deck (§8) |
| Scope creep into dashboards or streaming | Missed deadline | §11 |
| Service-account key or Kaggle token committed to git | Credential leak; code-quality penalty | `.gitignore` in the first commit; all secrets in GitHub Actions secrets and local `.env` (§10) |
| Dagster run history confined to one unreplicated VM | Lost observability if the machine is lost | Accepted and documented: history is durable under a bind-mounted `DAGSTER_HOME` and survives redeploys and reboots, but is not backed up or readable off-host. The citable evidence remains the dbt docs published to Pages (§8) |
| The daemon is stopped to reclaim memory on the `e2-micro` | The dbt layer silently stops building while ingestion still reports green | The daemon evaluates the automation sensor, not just the cron — documented at the service definition in `docker-compose.vm.yml`, and both triggers default to `RUNNING` (§8) |
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
