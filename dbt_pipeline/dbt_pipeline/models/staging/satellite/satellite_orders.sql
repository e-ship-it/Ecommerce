{{ config(
    materialized='incremental',
    unique_key='order_hkey'
) }}

with staged as (
    select
        md5(TRIM(order_id::text)) as order_hkey,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        created_at,
        updated_at,
        CURRENT_TIMESTAMP as load_timestamp,
        md5(concat_ws('||', TRIM(order_number::text), TRIM(order_dow::text), TRIM(order_hour_of_day::text), TRIM(days_since_prior_order::text))) as hashdiff
    from {{ source('streams', 'orders') }}
),

latest as (

    {% if is_incremental() %}

    select order_hkey,hashdiff from (
        select
        order_hkey,
        hashdiff,
        row_number() over (partition by order_hkey order by load_timestamp desc) as rn
    from {{ this }}) t
    where rn = 1

    {% else %}

    select
        null::TEXT as order_hkey,
        null::TEXT as hashdiff
    where false

    {% endif %}
),

to_insert as (
    select s.*
    from staged s
    left join latest l
      on s.order_hkey = l.order_hkey
     and s.hashdiff = l.hashdiff
    where l.order_hkey is null

{% if is_incremental() %}
  and load_timestamp > (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
{% endif %}
)

select * from to_insert