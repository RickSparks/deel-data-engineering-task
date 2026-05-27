with source as (
    select * from {{ source('raw', 'customers') }}
    where _op != 'd'
)

select
    customer_id,
    customer_name,
    is_active,
    customer_address,
    updated_at,
    created_at,
    _ingested_at
from source