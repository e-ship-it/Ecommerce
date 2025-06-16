{{ config(
    materialized='incremental',
    unique_key='user_hkey',
    schema='opts_hub'
) }}

{% if is_incremental() %}

with latest_sat as (
    select
        s.user_hkey,
        h.user_id,
        s.user_name,
        s.email,
        s.phone_number,
        s.address,
        s.birthdate,
        s.registration_date,
        s.load_timestamp,
        row_number() over (partition by s.user_hkey order by s.load_timestamp desc) as rn
    from {{ ref('satellite_user') }} s
    join {{ ref('hub_users') }} h
      on s.user_hkey = h.user_hkey
),

current_records as (
    select * from latest_sat where rn = 1
),

existing_dim as (
    select * from {{ this }}
),

changes as (
    select
        (COALESCE((SELECT MAX(DIM_USER_ID) FROM existing_dim), 0) + ROW_NUMBER() OVER (ORDER BY c.load_timestamp)) AS DIM_USER_ID,
        c.user_hkey,
        c.user_id,
        c.user_name,
        c.email,
        c.phone_number,
        c.address,
        c.birthdate,
        c.registration_date,
        c.load_timestamp as valid_from,
        cast('9999-12-31' as timestamp) as valid_to
    from current_records c
    left join existing_dim e
      on c.user_hkey = e.user_hkey and e.valid_to = cast('9999-12-31' as timestamp)
    where
      e.user_hkey is null
      or (
        c.user_name is distinct from e.user_name
        or c.email is distinct from e.email
        or c.phone_number is distinct from e.phone_number
        or c.address is distinct from e.address
        or c.birthdate is distinct from e.birthdate
        or c.registration_date is distinct from e.registration_date
      )
),

expired_old as (
    select e.*
    from existing_dim e
    join changes c on e.user_hkey = c.user_hkey
    where e.valid_to = cast('9999-12-31' as timestamp)
),

expired_old_update as (
    select
        e.DIM_USER_ID,
        e.user_hkey,
        e.user_id,
        e.user_name,
        e.email,
        e.phone_number,
        e.address,
        e.birthdate,
        e.registration_date,
        e.valid_from,
        c.valid_from - interval '1 second' as valid_to
    from expired_old e
    join changes c on e.user_hkey = c.user_hkey
)

select * from expired_old_update

union all

select DIM_USER_ID,
    user_hkey,
    user_id,
    user_name,
    email,
    phone_number,
    address,
    birthdate,
    registration_date,
    valid_from,
    valid_to
from changes
where valid_from > (
    select coalesce(max(valid_from), '1900-01-01'::timestamp) from {{ this }}
)

{% else %}

with latest_sat as (
    select (ROW_NUMBER() OVER (ORDER BY s.load_timestamp)) AS DIM_USER_ID,
        s.user_hkey,
        h.user_id,
        s.user_name,
        s.email,
        s.phone_number,
        s.address,
        s.birthdate,
        s.registration_date,
        s.load_timestamp,
        row_number() over (partition by s.user_hkey order by s.load_timestamp desc) as rn
    from {{ ref('satellite_user') }} s
    join {{ ref('hub_users') }} h
      on s.user_hkey = h.user_hkey
),

current_records as (
    select * from latest_sat where rn = 1
)

select DIM_USER_ID,
    user_hkey,
    user_id,
    user_name,
    email,
    phone_number,
    address,
    birthdate,
    registration_date,
    load_timestamp as valid_from,
    cast('9999-12-31' as timestamp) as valid_to
from current_records

{% endif %}
