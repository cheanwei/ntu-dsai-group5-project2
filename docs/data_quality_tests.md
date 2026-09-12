# Data-quality tests

All warehouse checks run through dbt. `dbt build` creates models and runs the
tests in dependency order; Dagster exposes the results as asset checks.

The current manifest contains 79 tests.

## Coverage

| Check | Main targets | Failure meaning |
|---|---|---|
| `unique`, `not_null` | source IDs, model keys, required foreign keys | A declared grain or required join key is broken |
| `relationships` | order, customer, product, seller, payment, and review links | A child row has no expected parent |
| `accepted_values` | order status and payment type | An unexpected business value appeared |
| `dbt_utils.unique_combination_of_columns` | order items and payments | A composite-grain fact has duplicates |
| `dbt_utils.expression_is_true` | non-negative amounts, review scores, customer grain | A numeric domain or modeling invariant failed |
| `dbt_expectations` ranges | fact row counts, dates, payments, installments, reviews | A load is partial or outside its expected domain |
| `dbt_expectations` column ordering | order lifecycle timestamps | Source timestamps are out of sequence |

Most tests fail the build. Lifecycle ordering and high-installment checks use
warning severity because the source contains plausible historical anomalies;
they should remain visible without blocking unrelated models.

## Key analytical protections

- `dim_customer` must have fewer rows than `fct_orders`, guarding against use
  of the per-order `customer_id` as a customer key.
- `fct_order_items` is unique at `order_id + order_item_id`.
- `fct_payments` is unique at `order_id + payment_sequential`.
- `fct_orders` must contain 90,000–110,000 rows after a complete Olist load.
- Review scores remain in the 1–5 domain.
- `int_order_payment_totals` exposes `payment_difference` and
  `is_payment_reconciled` at order grain. A blocking test on that flag has not
  yet been added.

## Run the checks

From the repository root:

```bash
set -a; source .env; set +a
uv run dbt deps --project-dir transform --profiles-dir transform
uv run dbt build --project-dir transform --profiles-dir transform
```

For tests only:

```bash
uv run dbt test --project-dir transform --profiles-dir transform
```

Test declarations live in:

- `transform/models/staging/_sources.yml`
- `transform/models/{staging,intermediate,marts}/schema.yml`

`transform/tests/` is reserved for singular SQL tests; none are currently
implemented there.
