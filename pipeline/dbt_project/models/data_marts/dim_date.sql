{{ config(materialized='table') }}

select
    to_char(d, 'YYYYMMDD')::integer as date_key,
    d as full_date,
    trim(to_char(d, 'Day')) as day_of_week,
    extract(day from d)::integer as day_of_month,
    extract(month from d)::integer as month_number,
    trim(to_char(d, 'Month')) as month_name,
    extract(quarter from d)::integer as quarter,
    extract(year from d)::integer as year
from
    generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval) as d