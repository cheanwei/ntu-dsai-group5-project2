{{ config(
    partition_by = {'field': 'order_purchase_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['customer_key', 'order_status']
) }}

-- Accumulating snapshot at order grain (~99,441 rows).
-- One row per order, with the lifecycle timestamps and deltas from
-- int_order_lifecycle and the reconciled totals from
-- int_order_payment_totals.
-- Owner: lane A2.

with lifecycle as (

    select * from {{ ref('int_order_lifecycle') }}

),

payment_totals as (

    select * from {{ ref('int_order_payment_totals') }}

),

final as (

    select
        lifecycle.order_id,
        lifecycle.customer_key,
        lifecycle.order_purchase_date,
        lifecycle.order_purchase_timestamp,
        lifecycle.order_status,
        lifecycle.order_approved_at,
        lifecycle.order_delivered_carrier_date,
        lifecycle.order_delivered_customer_date,
        lifecycle.order_estimated_delivery_date,
        lifecycle.approval_days,
        lifecycle.handover_days,
        lifecycle.delivery_days,
        lifecycle.estimated_vs_actual_days,
        lifecycle.is_late,
        lifecycle.is_revenue_order,
        payment_totals.item_count,
        payment_totals.payment_count,
        payment_totals.item_price_total,
        payment_totals.freight_value_total,
        payment_totals.item_gross_total,
        payment_totals.payment_value_total,
        payment_totals.payment_difference,
        payment_totals.is_payment_reconciled

    from lifecycle
    left join payment_totals using (order_id)

)

select * from final
