{{
  config(
    materialized = 'view'
  )
}}

with user_events as (
    select * from {{ ref('stg_ga4__events') }}
),

users as (
    select
        user_pseudo_id,
        min(event_timestamp) as first_event_timestamp,
        max(event_timestamp) as last_event_timestamp,
        count(distinct ga_session_id) as total_sessions,
        count(event_name) as total_events
    from user_events
    group by 1
)

select * from users