with staging as (
    select * from {{ ref('stg_constituencies') }}
)

select
    constituency_id,
    constituency_name,
    member_of_parliament
from staging
