with source as (
    select * from {{ source('raw_sources', 'readings') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by site_id, date_time
            order by id asc
        ) as row_num
    from source
)

select
    id as reading_id,
    date_time as observation_timestamp,
    site_id as station_id,
    nox,
    no2,
    no,
    pm10,
    o3,
    temp as temperature_c,
    nvpm10,
    vpm10,
    nvpm2_5,
    pm2_5,
    vpm2_5,
    co,
    rh as relative_humidity,
    pressure as air_pressure,
    so2,
    row_checksum
from deduplicated
where row_num = 1
