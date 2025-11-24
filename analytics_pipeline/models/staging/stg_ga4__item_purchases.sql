with unnested_data as (

    select
        event_date,
        event_timestamp,
        user_pseudo_id,
        (select value.string_value from unnest(event_params) where key = 'transaction_id') as transaction_id,
        item.item_id,
        item.item_name,
        item.price_in_usd
        
    from {{ source('ga4_sample', 'events_20210131') }}, unnest(items) as item 
    where event_name = 'purchase'

)

select
    *
from unnested_data
where transaction_id is not null