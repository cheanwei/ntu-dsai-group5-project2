-- stg_product_category_translation: 1:1 with source `olist_raw.product_category_translation`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Portuguese to English category names.
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'product_category_translation') }}

),

renamed as (

    select
        lower(trim(product_category_name)) as product_category_name,
        lower(trim(product_category_name_english)) as product_category_name_english

    from source

)

select * from renamed
