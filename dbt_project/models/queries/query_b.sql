with readings as (
    select * from {{ ref('fact_reading') }}
),
stations as (
    select * from {{ ref('dim_station') }}
)

select
    s.station_name,
    avg(case when r.pm2_5 = 'NaN'::real then null else r.pm2_5 end) as mean_pm2_5,
    avg(case when r.vpm2_5 = 'NaN'::real then null else r.vpm2_5 end) as mean_vpm2_5
from readings r
join stations s on r.station_id = s.station_id
where extract(year from r.observation_timestamp) = 2019
  and extract(hour from r.observation_timestamp) = 8
group by s.station_name
order by s.station_name
