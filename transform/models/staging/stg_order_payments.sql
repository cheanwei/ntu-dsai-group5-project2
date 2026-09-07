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
        trim(order_id) as order_id,
        cast(payment_sequential as int64) as payment_sequential,
        lower(trim(payment_type)) as payment_type,
        cast(payment_installments as int64) as payment_installments,
        cast(payment_value as numeric) as payment_value

    from source

)

select * from renamed
