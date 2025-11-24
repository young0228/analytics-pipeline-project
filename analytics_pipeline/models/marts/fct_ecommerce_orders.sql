{{
  config(
    materialized = 'table' 
  )
}}

with item_purchases as (
    select * from {{ ref('stg_ga4__item_purchases') }}
),

orders_aggregated as (
    select
        transaction_id,
        user_pseudo_id,
        min(event_timestamp) as order_timestamp,
        sum(price_in_usd) as total_transaction_revenue_usd,
        count(distinct item_id) as item_count
    from item_purchases
    group by 1, 2
)

select
    transaction_id,
    user_pseudo_id,
    order_timestamp,
    total_transaction_revenue_usd as gmv_usd,
    item_count,
    (total_transaction_revenue_usd / item_count) as avg_item_price_usd

from orders_aggregated