-- Payment reconciliation at order grain.
--
-- Payments are at order grain, items at line grain. Joining them multiplies
-- payment_value by the number of lines in the order. Aggregating each side to
-- order grain here — and only here — is the correct handling; the fan-out is a
-- documented trap (§5.2).
--
-- Expect >=99% of orders to reconcile within tolerance. Vouchers and partial
-- payments produce genuine mismatches: investigate and document the residual,
-- never suppress it (§7, GX check 1).
-- Owner: lane A2.

with payments as (

    select * from {{ ref('stg_order_payments') }}

),

items as (

    select * from {{ ref('stg_order_items') }}

),

reconciled as (

    select
        -- TODO(A2): sum(payment_value) vs sum(price + freight_value) per
        -- order_id, plus the difference and a within-tolerance flag.
        *

    from payments

)

select * from reconciled
