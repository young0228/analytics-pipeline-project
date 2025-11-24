select
    event_date,
    event_timestamp,
    event_name,
    (select value.string_value from unnest(event_params) where key = 'page_title') as page_title,
    (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
    user_pseudo_id
    
from {{ source('ga4_sample', 'events_20210131') }}