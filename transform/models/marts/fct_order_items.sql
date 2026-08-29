{{ config(
    partition_by = {'field': 'order_purchase_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['product_key', 'seller_key']
) }}

-- The atomic sales fact. Grain: order_id + order_item_id (~112,650 rows).
--
-- Physical design per §5.3: every meaningful analytical query filters on a
-- purchase date range and groups by product or seller. At 120 MB this will not
-- measurably speed anything up — it is declared because the grain and access
-- pattern would demand it at 1000x, and the cost of designing for that now is
-- zero. State the sample size openly (§3).
--
-- Payments are deliberately NOT joined here: they are at order grain, and
-- joining would multiply payment_value by the number of lines (§5.2).
-- Owner: lane A2.

with items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

final as (

    select
        -- TODO(A2): order_id, order_item_id, customer_key, product_key,
        -- seller_key, order_purchase_date, price, freight_value.
        *

    from items

)

select * from final
