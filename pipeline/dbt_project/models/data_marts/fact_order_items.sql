-- depends_on: {{ ref('snap_orders') }}
-- depends_on: {{ ref('snap_products') }}
-- depends_on: {{ ref('snap_customers') }}

{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='append_new_columns'
    )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    -- On incremental runs, process newer records
    where _ingested_at > (
        select COALESCE(max(_ingested_at), '1970-01-01'::timestamp) 
        from {{ this }}
    )
{% endif %}
),

orders as (
    -- to get order state
    select * from {{ ref('snap_orders') }}
    where dbt_valid_to is null  -- current version only
),

products as (
    select * from {{ ref('snap_products') }}
    where dbt_valid_to is null
),

customers as (
    select * from {{ ref('snap_customers') }}
    where dbt_valid_to is null
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    -- Date keys for joining to dim_date
    od.date_key as order_date_key,
    dd.date_key as delivery_date_key,
    -- Metrics/KPIs
    oi.quantity,
    p.unity_price,
    (oi.quantity * p.unity_price)                as total_amount,
    o.status                                     as order_status,
    o.delivery_date,
    -- Metadata
    oi.created_at                                as source_created_at,
    oi.updated_at                                as source_updated_at,
    oi._ingested_at

from order_items oi
left join orders o on oi.order_id = o.order_id
left join products p on oi.product_id = p.product_id
left join customers c on o.customer_id = c.customer_id
left join dim_date od on o.order_date::date = od.full_date
left join dim_date dd on o.delivery_date = dd.full_date