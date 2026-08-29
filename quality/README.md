# Great Expectations — Tier 2 quality

Tier 1 is dbt tests: fast, structural, inline in every build. Tier 2 is here —
the cross-table and statistical assertions dbt expresses awkwardly, run after
the marts build (§7).

GX also emits **Data Docs**, a browsable HTML quality report. That artifact is
the point: it is concrete evidence for the documentation criterion in a way
that `dbt test` console output is not. It is published to GitHub Pages by
`.github/workflows/pipeline.yml` with `if: always()`, because it matters most
when a run fails (§8).

Results surface in Dagster as **asset checks**, so a quality failure appears
against the asset that produced it rather than as an unrelated task failure.

**Verify, never repair.** A test that fixes what it found hides the defect (§6).

## The five suites (§7)

1. **Payment reconciliation** — per order, `sum(payment_value)` vs
   `sum(price + freight_value)` within tolerance. Expect >=99% to pass.
   Investigate and *document* the residual: vouchers and partial payments
   produce genuine mismatches.
2. **Date monotonicity** — purchase <= approved <= carrier handover <=
   delivered. Known violations exist in the source; quantify them.
3. **Row-count stability** — fact tables within an expected band, catching a
   partial load.
4. **Referential completeness** — every `fct_order_items.customer_key`
   resolves.
5. **Distribution checks** — mean `review_score` plausible; monthly order
   volume non-zero across the window.

## Layout

    great_expectations/
      great_expectations.yml     # context config; BigQuery datasource
      expectations/              # the five suites
      checkpoints/               # what the Dagster asset check runs
      uncommitted/               # gitignored
        data_docs/               # generated HTML, published to Pages

Owner: lane B1. Week 1 the suites are written as prose in this README; week 3
they become code.
