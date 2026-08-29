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
        -- TODO(A2): product_key, category (pt + en, coalesced to 'unknown'),
        -- dimensions, weight, photo count.
        *

    from products

)

select * from final
