-- models/staging/stg_passengers.sql
with source as (
    select *
    from {{ ref('passengers_raw') }}
)

select
    passenger_id,
    name,
    to_date(join_date) as join_date,
    rating
from source
