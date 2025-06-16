{{ config(
    materialized='incremental',
    unique_key='user_hkey'
) }}

with staged as (
    select
        md5(TRIM(user_id::TEXT)) as user_hkey,
        name as user_name,
        email,
        phone_number,
        address,
        birthdate,
        registration_date,
        CURRENT_TIMESTAMP as load_timestamp,
        md5(concat_ws('||', TRIM(name), TRIM(email), TRIM(phone_number), TRIM(address), TRIM(birthdate::TEXT), TRIM(registration_date::TEXT))) as hashdiff
    from {{ source('batch', 'users') }}
),

latest as (

    {% if is_incremental() %}

    select user_hkey, hashdiff from (
        select
        user_hkey,
        hashdiff,
        row_number() over (partition by user_hkey order by load_timestamp desc) as rn
    from {{ this }}) t
    where rn = 1

    {% else %}

    select
        null::TEXT as user_hkey,
        null::TEXT as hashdiff
    where false

    {% endif %}
),

to_insert as (
    select s.*
    from staged s
    left join latest l
      on s.user_hkey = l.user_hkey
     and s.hashdiff = l.hashdiff
    where l.user_hkey is null

{% if is_incremental() %}
  and load_timestamp > (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
{% endif %}
)

select * from to_insert
