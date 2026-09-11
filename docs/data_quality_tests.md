# Data-quality tests

This project uses dbt's built-in tests for keys, relationships, required
fields, and enumerated values. `dbt_expectations` adds Great
Expectations-style checks for value ranges, permitted sets, ordered timestamps,
and plausible table sizes.

## Basic structural checks

These low-cost tests run on raw sources and across the staging, intermediate,
and mart layers. They protect the declared grain and joins before analytical
quality checks run.

| Test | Applied to | What it protects |
|---|---|---|
| `unique` + `not_null` | Source `customers.customer_id` and `orders.order_id`; staging and mart dimension keys; `fct_orders.order_id`; intermediate order/customer keys | A declared primary key identifies exactly one record. |
| `not_null` | Required business and foreign keys, including customer, order-item, and payment keys; `dim_product.product_category_name_english` | Required fields are available for joins and reporting. |
| `relationships` | Order/customer, order-item/customer/product/seller, payment/order, and review/order foreign keys | Fact rows resolve to valid parent dimension or order records. |
| `accepted_values` | `order_status` and `payment_type` in staging and marts | Categorical values remain within the Olist business domain. |
| `dbt_utils.unique_combination_of_columns` | `order_id + order_item_id` and `order_id + payment_sequential` | Composite-grain facts do not contain duplicate lines or payment sequences. |
| `dbt_utils.expression_is_true` | Price and freight are non-negative; review scores are 1–5 | Basic numeric domains are valid. |
| `dbt_utils.expression_is_true` | `dim_customer` row count is lower than `fct_orders` row count | The customer dimension remains keyed by stable `customer_unique_id`, rather than the per-order surrogate. |

`unique` is only used where a column is expected to be a single-column key.
For order items and payments, uniqueness applies to the documented composite
grain instead.

## Current expectation checks

| Model | Check | Severity | Purpose |
|---|---|---|---|
| `fct_orders` | 90,000–110,000 rows | error | Detect an empty or partial Olist load. |
| `fct_orders.order_purchase_date` | non-null; 2016-09-01 through 2018-12-31 | error | Keep the analytical date spine within the extract period. |
| `fct_payments.payment_value` | at least 0 | error | Reject invalid negative payments. |
| `fct_payments.payment_installments` | 1–24 | warn | Surface unusual payment plans without blocking the build. |
| `fct_reviews.review_score` | one of 1–5 | error | Enforce the review-score domain. |
| `fct_reviews.review_creation_date` | non-null | error | Keep review trend reporting usable. |
| `int_order_lifecycle` | purchase <= approval <= carrier <= delivery, when both values exist | warn | Quantify known source timestamp anomalies. |
| `int_order_payment_totals.is_within_tolerance` | always true | error | Prevent payment/item fan-out or reconciliation failures. |

The lifecycle and reconciliation columns are part of the intended intermediate
model contracts. Keep their tests alongside the model when implementing those
columns; do not silently remove failing checks to accommodate a data issue.

## Running tests

From `transform`:

```bash
dbt deps
dbt test
```

Run only the expectation package's tests during iteration with:

```bash
dbt test --select "test_name:expect_*"
```

An `error` test fails the command. A `warn` test reports an anomaly but exits
successfully, which is appropriate for documented historical source issues.
