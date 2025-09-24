-- models/staging/stg_trips.sql
with source as (
    select *
    from {{ ref('trips_raw') }}
)

select
    trip_id,
    driver_id,
    passenger_id,
    to_timestamp(start_time) as start_time,
    to_timestamp(end_time) as end_time,
    distance_km,
    fare,
    rating
from source
