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
        -- TODO(A2): review_id, order_id, review_score, review_creation_date,
        -- response latency, has_comment.
        *

    from reviews

)

select * from final
