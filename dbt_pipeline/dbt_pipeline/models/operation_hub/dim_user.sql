{{ config(materialized='incremental', unique_key='user_hkey') }}

with satellite as (
    select 
        user_id,
        name,
        email,
        birthdate,
        registration_date,
        address,
        phone_number,
        created_at,
        updated_at,
        load_timestamp
    from {{ ref('satellite_user') }} a JOIN {{ ref('hub_users') }} b ON a.user_hkey = b.user_hkey
),

-- Using window function to pick the latest satellite record per user for current active version
latest_satellite as (
    select *,
        row_number() over (partition by user_hkey order by load_timestamp desc) as rn
    from satellite
),

-- Mark active and inactive rows for SCD2 (only keep latest active)
scd2_active as (
    select
        user_id,
        name,
        email,
        birthdate,
        registration_date,
        address,
        phone_number,
        created_at,
        updated_at,
        true as is_active,
        load_timestamp
    from latest_satellite
    where rn = 1
),

scd2_history as (
    select
        user_id,
        name,
        email,
        birthdate,
        registration_date,
        address,
        phone_number,
        created_at,
        updated_at,
        false as is_active,
        load_timestamp
    from latest_satellite
    where rn > 1
)

select * from scd2_active
union all
select * from scd2_history