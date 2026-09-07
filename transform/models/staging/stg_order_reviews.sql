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
        trim(review_id) as review_id,
        trim(order_id) as order_id,
        cast(review_score as int64) as review_score,
        nullif(trim(review_comment_title), '') as review_comment_title,
        nullif(trim(review_comment_message), '') as review_comment_message,
        cast(review_creation_date as timestamp) as review_creation_date,
        cast(review_answer_timestamp as timestamp) as review_answer_timestamp

    from source

),

deduped as (

    select * except (row_number)
    from (
        select
            *,
            row_number() over (
                partition by review_id
                order by review_answer_timestamp desc nulls last, order_id
            ) as row_number
        from renamed
    )
    where row_number = 1

)

select * from deduped
