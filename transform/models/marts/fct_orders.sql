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
        -- TODO(A2): order_id, customer_key, order_purchase_date,
        -- order_status, lifecycle deltas, item and payment totals.
        *

    from lifecycle

)

select * from final
