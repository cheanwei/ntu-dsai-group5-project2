# Singular tests

Bespoke assertions that do not fit a generic test, one SQL file each, returning
the rows that violate the assertion. Owner: lane B1.

Tier 1 lives here and in the `schema.yml` files — fast, structural, gating
every build. Cross-table and statistical assertions belong in Great
Expectations instead (`quality/`, §7).

**Tests verify; they never repair.** A test that fixes data hides the defect it
found (§6).

Candidates:

- `assert_no_orphan_order_items.sql` — every `fct_order_items.order_id`
  resolves to `fct_orders`.
- `assert_dim_customer_below_order_count.sql` — the §12 guard against keying
  the customer dimension on the per-order surrogate.
- `assert_purchase_before_delivery.sql` — date monotonicity, expected to find a
  known non-zero count in this source. Quantify it; do not suppress it.
