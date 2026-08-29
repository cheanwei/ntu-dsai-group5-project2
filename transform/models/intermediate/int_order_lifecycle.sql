-- Order lifecycle deltas: purchase -> approved -> carrier -> delivered.
--
-- Null handling is the substance of this model (§6): a null
-- order_delivered_customer_date means genuinely undelivered, so delivery_days
-- is null and is_late is NULL, NOT false. Conflating not-applicable with
-- broken either destroys real signal or hides real breakage.
--
-- Known monotonicity violations exist in the source; quantify them rather than
-- suppressing them (§7, GX check 2).
-- Owner: lane A2.

with orders as (

    select * from {{ ref('stg_orders') }}

),

lifecycle as (

    select
        -- TODO(A2): approval_days, handover_days, delivery_days,
        -- estimated_vs_actual_days, is_late (nullable), is_revenue_status
        -- using var('revenue_order_statuses').
        *

    from orders

)

select * from lifecycle
