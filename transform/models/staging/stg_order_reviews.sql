-- stg_order_reviews: 1:1 with source `olist_raw.order_reviews`.
-- Allowed here (§6): snake_case renames, casts, whitespace trim, single-table
-- dedupe. Prohibited here: cross-table logic, business rules, filtering rows
-- for business reasons.
-- Dedupe repeated review_id, keeping the latest review_answer_timestamp (§6).
-- Owner: lane A2. Descriptions: lane C1.

with source as (

    select * from {{ source('olist_raw', 'order_reviews') }}

),

renamed as (

    select
        -- TODO(A2): explicit column list — rename, cast, trim.
        *

    from source

)

select * from renamed
