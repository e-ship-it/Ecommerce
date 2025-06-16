{{ config(
    materialized='incremental',
    unique_key='aisle_department_product_hkey'
) }}

with staged as (
    select
        md5(TRIM(aisle_id::TEXT) || TRIM(department_id::TEXT) || TRIM(product_id::TEXT)) as aisle_department_product_hkey,
        product_name,
        created_at,
        updated_at,
        CURRENT_TIMESTAMP as load_timestamp,
        md5(concat_ws('||', product_name)) as hashdiff
    from {{ source('batch', 'products') }}
),

latest as (

    {% if is_incremental() %}

    select aisle_department_product_hkey,hashdiff from(
        select
        aisle_department_product_hkey,
        hashdiff,
        row_number() over (partition by aisle_department_product_hkey order by load_timestamp desc) as rn
    from {{ this }}) t
    where rn = 1

    {% else %}

    select
        null::TEXT as aisle_department_product_hkey,
        null::TEXT as hashdiff
    where false

    {% endif %}
),

to_insert as (
    select s.*
    from staged s
    left join latest l
      on s.aisle_department_product_hkey = l.aisle_department_product_hkey
     and s.hashdiff = l.hashdiff
    where l.aisle_department_product_hkey is null

{% if is_incremental() %}
  and load_timestamp > (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
{% endif %}
)

select * from to_insert