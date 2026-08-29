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
        -- TODO(A2): explicit column list — rename, cast, trim.
        *

    from source

)

select * from renamed
