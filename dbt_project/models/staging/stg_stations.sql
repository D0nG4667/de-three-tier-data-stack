select
    site_id as station_id,
    name as station_name,
    latitude,
    longitude,
    constituency_id,
    date_start,
    date_end,
    is_current,
    instrument_type
from {{ source('raw_sources', 'stations') }}
