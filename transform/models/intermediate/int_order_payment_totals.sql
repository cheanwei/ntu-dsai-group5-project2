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

orders as (

    select order_id from {{ ref('stg_orders') }}

),

payment_totals as (

    select
        order_id,
        sum(payment_value) as payment_value_total,
        count(*) as payment_count
    from payments
    group by order_id

),

item_totals as (

    select
        order_id,
        sum(price) as item_price_total,
        sum(freight_value) as freight_value_total,
        sum(price + freight_value) as item_gross_total,
        count(*) as item_count
    from items
    group by order_id

),

reconciled as (

    select
        orders.order_id,
        coalesce(payment_totals.payment_value_total, cast(0 as numeric)) as payment_value_total,
        coalesce(payment_totals.payment_count, 0) as payment_count,
        coalesce(item_totals.item_price_total, cast(0 as numeric)) as item_price_total,
        coalesce(item_totals.freight_value_total, cast(0 as numeric)) as freight_value_total,
        coalesce(item_totals.item_gross_total, cast(0 as numeric)) as item_gross_total,
        coalesce(item_totals.item_count, 0) as item_count,
        coalesce(payment_totals.payment_value_total, cast(0 as numeric))
            - coalesce(item_totals.item_gross_total, cast(0 as numeric)) as payment_difference,
        abs(coalesce(payment_totals.payment_value_total, cast(0 as numeric))
            - coalesce(item_totals.item_gross_total, cast(0 as numeric))) <= cast(0.01 as numeric)
            as is_payment_reconciled

    from orders
    left join payment_totals using (order_id)
    left join item_totals using (order_id)

)

select * from reconciled
