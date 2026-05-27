-- depends_on: {{ ref('snap_orders') }}

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'dbt_valid_from']) }}  as order_key,
    order_id,
    customer_id,
    order_date,
    delivery_date,
    status,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_orders') }}