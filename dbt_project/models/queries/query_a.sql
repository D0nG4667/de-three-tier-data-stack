with readings as (
    select * from {{ ref('fact_reading') }}
),
stations as (
    select * from {{ ref('dim_station') }}
)

select
    r.observation_timestamp as date_time,
    s.station_name,
    r.nox as highest_nox
from readings r
join stations s on r.station_id = s.station_id
where extract(year from r.observation_timestamp) = 2019
  and r.nox is not null
  and r.nox != 'NaN'::real
order by r.nox desc
limit 1
