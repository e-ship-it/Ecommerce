{{ config(
    materialized='incremental',
    unique_key='order_product_hkey'
) }}

with staged as (
    select
        md5(TRIM(order_id::TEXT) || TRIM(product_id::TEXT)) as order_product_hkey,
        reordered,
        add_to_cart_order,
        created_at,
        updated_at,
        CURRENT_TIMESTAMP as load_timestamp,
        md5(concat_ws('||', reordered::text, add_to_cart_order::text)) as hashdiff
    from {{ source('streams', 'user_order_activity_stream') }}
),

latest as (
    {% if is_incremental() %}

    select order_product_hkey,hashdiff from
    (select
        order_product_hkey,
        hashdiff,
        row_number() over (partition by order_product_hkey order by load_timestamp desc) as rn
    from {{ this }}) t
    where rn = 1

    {% else %}

    select
        null::TEXT as order_product_hkey,
        null::TEXT as hashdiff
    where false

    {% endif %}
),

to_insert as (
    select s.*
    from staged s
    left join latest l
      on s.order_product_hkey = l.order_product_hkey
     and s.hashdiff = l.hashdiff
    where l.order_product_hkey is null

{% if is_incremental() %}
  and load_timestamp > (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
{% endif %}
)

select * from to_insert
