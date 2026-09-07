-- stg_customers: 1:1 with source `olist_raw.customers`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Carry customer_unique_id through untouched; dim_customer keys on it (§5.2).
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'customers') }}

),

renamed as (

    select
        trim(customer_id) as customer_id,
        trim(customer_unique_id) as customer_unique_id,
        trim(customer_zip_code_prefix) as customer_zip_code_prefix,
        lower(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state

    from source

)

select * from renamed
