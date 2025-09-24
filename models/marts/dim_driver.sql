-- models/marts/dim_driver.sql
with base as (
    select
        d.driver_id,
        d.name,
        d.join_date,
        d.vehicle_type,
        d.rating as self_rating
    from {{ ref('stg_drivers') }} d
),

metrics as (
    select
        driver_id,
        count(*) as total_trips,
        avg(fare) as avg_fare,
        avg(trip_rating) as avg_trip_rating,
        avg(trip_duration_mins) as avg_trip_duration
    from {{ ref('fct_trips') }}
    group by driver_id
)

select
    b.*,
    m.total_trips,
    m.avg_fare,
    m.avg_trip_rating,
    m.avg_trip_duration
from base b
left join metrics m on b.driver_id = m.driver_id
