{{ config(cluster_by = ['product_key']) }}

-- Grain: product_id (~32,951 rows).
--
-- ~610 products have a null product_category_name with no legitimate reason.
-- Map them to an explicit 'unknown' member: a dimension attribute used for
-- grouping should never be null (§6). That is unknown-member handling, which
-- is the one thing marts are allowed to do — not cleaning.
-- Owner: lane A2.

with products as (

    select * from {{ ref('stg_products') }}

),

translation as (

    select * from {{ ref('stg_product_category_translation') }}

),

final as (

    select
        products.product_id as product_key,
        coalesce(products.product_category_name, 'unknown') as product_category_name,
        coalesce(translation.product_category_name_english, 'unknown')
            as product_category_name_english,
        products.product_name_length,
        products.product_description_length,
        products.product_photos_qty,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm

    from products
    left join translation using (product_category_name)

)

select * from final
