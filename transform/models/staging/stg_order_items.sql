-- stg_order_items: 1:1 with source `olist_raw.order_items`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Grain: order_id + order_item_id. price and freight_value stay separate.
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'order_items') }}

),

renamed as (

    select
        trim(order_id) as order_id,
        cast(order_item_id as int64) as order_item_id,
        trim(product_id) as product_id,
        trim(seller_id) as seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        cast(price as numeric) as price,
        cast(freight_value as numeric) as freight_value

    from source

)

select * from renamed
