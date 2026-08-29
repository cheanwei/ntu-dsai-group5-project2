{{ config(cluster_by = ['order_id']) }}

-- Grain: order_id + payment_sequential (~103,886 rows).
-- Not partitioned: no date column at this grain worth partitioning on (§5.3).
--
-- Kept separate from fct_order_items on purpose. Joining order-grain payments
-- to line-grain items multiplies payment_value by the number of lines and
-- inflates revenue; reconciliation happens in int_order_payment_totals (§5.2).
-- Owner: lane A2.

with payments as (

    select * from {{ ref('stg_order_payments') }}

),

final as (

    select
        -- TODO(A2): order_id, payment_sequential, payment_type,
        -- payment_installments, payment_value.
        *

    from payments

)

select * from final
