-- Per-customer order history, keyed on customer_unique_id.
--
-- The key is the point of this model. customer_id is a per-order surrogate;
-- rolling up on it yields ~99k customers who each ordered once and the false
-- conclusion that Olist has no repeat business. The true figure is ~96k unique
-- customers with roughly 3% repeating (§5.2). Any RFM segmentation built on
-- the wrong key is meaningless.
-- Owner: lane A2.

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select order_id, payment_value_total
    from {{ ref('int_order_payment_totals') }}

),

history as (

    select
        customers.customer_unique_id,
        -- A customer can have multiple order-level customer_id records. Keep
        -- the address on their most recent observed order, deterministically.
        array_agg(
            customers.customer_city order by orders.order_purchase_timestamp desc limit 1
        )[safe_offset(0)] as customer_city,
        array_agg(
            customers.customer_state order by orders.order_purchase_timestamp desc limit 1
        )[safe_offset(0)] as customer_state,
        array_agg(
            customers.customer_zip_code_prefix order by orders.order_purchase_timestamp desc limit 1
        )[safe_offset(0)] as customer_zip_code_prefix,
        min(date(orders.order_purchase_timestamp)) as first_order_date,
        max(date(orders.order_purchase_timestamp)) as last_order_date,
        count(*) as order_count,
        countif(orders.order_status = 'delivered') as delivered_order_count,
        countif(orders.order_status = 'delivered') > 1 as is_repeat_customer,
        sum(case when orders.order_status in ('delivered', 'shipped', 'invoiced', 'processing')
            then payments.payment_value_total else cast(0 as numeric) end) as lifetime_revenue,
        date_diff(date('{{ var("dataset_end_date") }}'), max(date(orders.order_purchase_timestamp)), day)
            as recency_days

    from customers
    inner join orders using (customer_id)
    left join payments using (order_id)
    group by customers.customer_unique_id

)

select * from history
