-- models/marts/dim_passenger.sql
with base as (
    select
        p.passenger_id,
        p.name,
        p.join_date,
        p.rating as self_rating
    from {{ ref('stg_passengers') }} p
),

metrics as (
    select
        passenger_id,
        count(*) as total_trips,
        avg(fare) as avg_fare_paid,
        avg(trip_rating) as avg_trip_rating_received,
        avg(trip_duration_mins) as avg_trip_duration
    from {{ ref('fct_trips') }}
    group by passenger_id
)

select
    b.*,
    m.total_trips,
    m.avg_fare_paid,
    m.avg_trip_rating_received,
    m.avg_trip_duration
from base b
left join metrics m on b.passenger_id = m.passenger_id
