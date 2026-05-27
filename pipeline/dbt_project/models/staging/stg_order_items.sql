with source as (
    select * from {{ source('raw', 'order_items') }}
    where _op != 'd'
)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    updated_at,
    created_at,
    _ingested_at
from source