-- models/marts/fct_trips.sql
with base as (
    select
        t.trip_id,
        t.driver_id,
        t.passenger_id,
        t.start_time,
        t.end_time,
        datediff('minute', t.start_time, t.end_time) as trip_duration_mins,
        t.distance_km,
        t.fare,
        t.rating as trip_rating
    from {{ ref('stg_trips') }} t
),

enriched as (
    select
        b.*,
        b.fare / nullif(b.distance_km,0) as fare_per_km,
        b.distance_km / nullif((b.trip_duration_mins/60),1) as avg_speed_kmh,
        case when b.distance_km > 20 then 1 else 0 end as is_long_trip,
        d.rating as driver_rating,
        p.rating as passenger_rating
    from base b
    left join {{ ref('stg_drivers') }} d on b.driver_id = d.driver_id
    left join {{ ref('stg_passengers') }} p on b.passenger_id = p.passenger_id
)

select * from enriched
