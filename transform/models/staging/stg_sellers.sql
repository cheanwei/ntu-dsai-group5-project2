-- stg_sellers: 1:1 with source `olist_raw.sellers`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- seller_zip_code_prefix stays STRING (§4).
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'sellers') }}

),

renamed as (

    select
        trim(seller_id) as seller_id,
        trim(seller_zip_code_prefix) as seller_zip_code_prefix,
        lower(trim(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state

    from source

)

select * from renamed
