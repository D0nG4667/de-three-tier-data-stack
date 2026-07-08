with readings as (
    select * from {{ ref('stg_readings') }}
)

select
    reading_id,
    observation_timestamp,
    station_id,
    nox,
    no2,
    no,
    pm10,
    o3,
    temperature_c,
    nvpm10,
    vpm10,
    nvpm2_5,
    pm2_5,
    vpm2_5,
    co,
    relative_humidity,
    air_pressure,
    so2,
    row_checksum,
    
    -- Derive Air Quality Band based on UK DEFRA Hourly NO2 objective
    case
        when no2 is null then 'Unknown'
        when no2 >= 0 and no2 <= 200 then 'Low'
        when no2 > 200 and no2 <= 400 then 'Moderate'
        when no2 > 400 and no2 <= 600 then 'High'
        else 'Very High'
    end as no2_air_quality_band,
    
    -- Derive UK Air Quality Index based on exact NO2 thresholds
    case
        when no2 is null then null
        when no2 >= 0 and no2 <= 67 then 1
        when no2 > 67 and no2 <= 134 then 2
        when no2 > 134 and no2 <= 200 then 3
        when no2 > 200 and no2 <= 267 then 4
        when no2 > 267 and no2 <= 334 then 5
        when no2 > 334 and no2 <= 400 then 6
        when no2 > 400 and no2 <= 467 then 7
        when no2 > 467 and no2 <= 534 then 8
        when no2 > 534 and no2 <= 600 then 9
        else 10
    end as no2_air_quality_index

from readings
