with stations as (
    select * from {{ ref('stg_stations') }}
),
constituencies as (
    select * from {{ ref('stg_constituencies') }}
)

select
    s.station_id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.date_start,
    s.date_end,
    s.is_current,
    s.instrument_type,
    c.constituency_name,
    c.member_of_parliament
from stations s
left join constituencies c on s.constituency_id = c.constituency_id
