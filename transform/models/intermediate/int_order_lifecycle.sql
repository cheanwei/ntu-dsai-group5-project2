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

customers as (

    select customer_id, customer_unique_id
    from {{ ref('stg_customers') }}

),

lifecycle as (

    select
        orders.order_id,
        customers.customer_unique_id as customer_key,
        orders.order_status,
        orders.order_purchase_timestamp,
        date(orders.order_purchase_timestamp) as order_purchase_date,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,
        timestamp_diff(orders.order_approved_at, orders.order_purchase_timestamp, hour) / 24.0
            as approval_days,
        timestamp_diff(orders.order_delivered_carrier_date, orders.order_approved_at, hour) / 24.0
            as handover_days,
        timestamp_diff(orders.order_delivered_customer_date, orders.order_purchase_timestamp, hour) / 24.0
            as delivery_days,
        timestamp_diff(
            orders.order_delivered_customer_date, orders.order_estimated_delivery_date, hour
        ) / 24.0 as estimated_vs_actual_days,
        case
            when orders.order_delivered_customer_date is null
              or orders.order_estimated_delivery_date is null then null
            when orders.order_delivered_customer_date > orders.order_estimated_delivery_date then true
            else false
        end as is_late,
        orders.order_status in (
            {% for status in var('revenue_order_statuses') %}
                '{{ status }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        ) as is_revenue_order

    from orders
    inner join customers using (customer_id)

)

select * from lifecycle
