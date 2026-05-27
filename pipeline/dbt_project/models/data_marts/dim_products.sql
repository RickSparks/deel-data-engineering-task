-- depends_on: {{ ref('snap_products') }}

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}    as product_key,
    product_id,
    product_name,
    barcode,
    unity_price,
    is_active,
    dbt_valid_from                                            as valid_from,
    dbt_valid_to                                              as valid_to,
    case when dbt_valid_to is null then true else false end    as is_current
from {{ ref('snap_products') }}