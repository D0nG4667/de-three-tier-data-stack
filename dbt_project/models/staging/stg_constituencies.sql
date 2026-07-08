select
    id as constituency_id,
    name as constituency_name,
    mp_name as member_of_parliament
from {{ source('raw_sources', 'constituencies') }}
