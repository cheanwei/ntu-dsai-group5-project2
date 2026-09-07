{{ config(
    partition_by = {'field': 'review_creation_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['order_id']
) }}

-- Grain: one review (~99,224 rows).
-- review_comment_message is frequently and legitimately null; keep it and add
-- has_comment for analysis (§6).
-- Owner: lane A2.

with reviews as (

    select * from {{ ref('stg_order_reviews') }}

),

final as (

    select
        review_id,
        order_id,
        review_score,
        date(review_creation_date) as review_creation_date,
        review_creation_date as review_creation_timestamp,
        review_answer_timestamp,
        timestamp_diff(review_answer_timestamp, review_creation_date, hour) / 24.0
            as review_response_days,
        review_comment_title,
        review_comment_message,
        review_comment_title is not null or review_comment_message is not null as has_comment

    from reviews

)

select * from final
