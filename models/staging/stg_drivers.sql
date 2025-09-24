-- models/staging/stg_drivers.sql
with source as (
    select *
    from {{ ref('drivers_raw') }}
)

select
    driver_id,
    name,
    to_date(join_date) as join_date,
    vehicle_type,
    rating
from source
