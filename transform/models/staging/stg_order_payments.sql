-- stg_order_payments: 1:1 with source `olist_raw.order_payments`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Grain: order_id + payment_sequential. Do not aggregate here.
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'order_payments') }}

),

renamed as (

    select
        -- TODO(A2): explicit column list — rename, cast, trim.
        *

    from source

)

select * from renamed
