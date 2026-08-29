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
        -- TODO(A2): explicit column list — rename, cast, trim.
        *

    from source

)

select * from renamed
