# Tier 2 — business invariants

Singular tests live here: one `.sql` file per assertion, each selecting the
rows that violate it. Empty result means pass. Column- and table-level
assertions that fit dbt's generic-test shape belong in
`models/marts/schema.yml` instead, next to the column they guard.

Both tiers are dbt tests and both run in `dbt build`, which is the whole reason
this is not a second framework (§7). `dbt_expectations` supplies the
Great Expectations-style macros — distributions, row-count bands, column pair
comparisons — that plain dbt expresses awkwardly.

Results reach Dagster as **asset checks** with no wiring: dagster-dbt models
every dbt test as a check on the asset it guards, so a failing payment
reconciliation lands on `fct_orders` rather than on a nameless task.

**Verify, never repair.** A test that fixes what it found hides the defect (§6).

## The five assertions (§7)

1. **Payment reconciliation** — per order, `sum(payment_value)` vs
   `sum(price + freight_value)` within tolerance. Expect >=99% to pass.
   Investigate and *document* the residual: vouchers and partial payments
   produce genuine mismatches. Singular test — it spans two facts.
2. **Date monotonicity** — purchase <= approved <= carrier handover <=
   delivered. Known violations exist in the source; quantify them.
   `dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B`.
3. **Row-count stability** — fact tables within an expected band, catching a
   partial load. `dbt_expectations.expect_table_row_count_to_be_between`.
4. **Referential completeness** — every `fct_order_items.customer_key`
   resolves. Already covered by the tier-1 `relationships` tests.
5. **Distribution checks** — mean `review_score` plausible; monthly order
   volume non-zero across the window.
   `dbt_expectations.expect_column_mean_to_be_between`.

## Named candidates

- `assert_payment_reconciliation.sql` — assertion 1, per order.
- `assert_no_orphan_order_items.sql` — every `fct_order_items.order_id`
  resolves to `fct_orders`.
- `assert_dim_customer_below_order_count.sql` — the §12 guard against keying
  the customer dimension on the per-order surrogate.
- `assert_purchase_before_delivery.sql` — date monotonicity, expected to find a
  known non-zero count in this source. Quantify it; do not suppress it.

Owner: lane B1. Blocked on lane A2: the marts models are still `select *`
stubs, so the columns these assert against do not exist yet.
