with source as (
    select * from {{ source('raw', 'products') }}
    where _op != 'd'
)

select
    product_id,
    product_name,
    barcode,
    unity_price,
    is_active,
    updated_at,
    created_at,
    _ingested_at
from source