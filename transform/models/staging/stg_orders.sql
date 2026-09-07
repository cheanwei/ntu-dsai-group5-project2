-- stg_orders: 1:1 with source `olist_raw.orders`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Five timestamp columns; nulls in order_delivered_customer_date are genuine (§6).
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'orders') }}

),

renamed as (

    select
        trim(order_id) as order_id,
        trim(customer_id) as customer_id,
        lower(trim(order_status)) as order_status,
        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date

    from source

)

select * from renamed
