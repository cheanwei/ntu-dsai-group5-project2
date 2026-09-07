-- stg_products: 1:1 with source `olist_raw.products`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Rename the source's misspelled product_name_lenght / product_description_lenght.
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'products') }}

),

renamed as (

    select
        trim(product_id) as product_id,
        nullif(lower(trim(product_category_name)), '') as product_category_name,
        cast(product_name_lenght as int64) as product_name_length,
        cast(product_description_lenght as int64) as product_description_length,
        cast(product_photos_qty as int64) as product_photos_qty,
        cast(product_weight_g as int64) as product_weight_g,
        cast(product_length_cm as int64) as product_length_cm,
        cast(product_height_cm as int64) as product_height_cm,
        cast(product_width_cm as int64) as product_width_cm

    from source

)

select * from renamed
