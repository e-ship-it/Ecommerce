{{ config(
    materialized='incremental',  -- Incremental load to only process new or updated records
    unique_key='user_hkey',      -- The unique key to identify each record
    incremental_strategy='merge',  -- Use merge strategy for inserts/updates
    schema='staging'             -- Target schema for the satellite table
) }}

with source_data as (
    -- Step 1: Get new or updated records from the source (batch.users)
    select
        md5(user_id::TEXT) as user_hkey,  -- Surrogate hash key for user_id
        MD5(
            TRIM(name) || '|' || TRIM(email) || '|' || COALESCE(birthdate::text, '') || '|' || 
            COALESCE(registration_date::text, '') || '|' || COALESCE(TRIM(address), '') || '|' || 
            COALESCE(TRIM(phone_number), '')
        ) AS diff_hkey,  -- Hash of user attributes that could change
        TRIM(name) as user_name,
        TRIM(email) as email,
        birthdate,
        registration_date,
        TRIM(address) as address,
        TRIM(phone_number) as phone_number,
        created_at,
        updated_at
    from {{ source('batch','users') }}  -- Reference source table
    where created_at > coalesce(
        (select max(created_at) from {{ this }}),  -- Compare with the last loaded timestamp
        '1900-01-01'::timestamp  -- Default date if the table is empty
    )
),

-- Step 2: Flag old records as inactive and insert updated ones
merge_data as (
    select
        s.user_hkey,
        s.user_name,
        s.email,
        s.birthdate,
        s.registration_date,
        s.address,
        s.phone_number,
        s.created_at,
        s.updated_at,
        s.load_timestamp,
        case
            when t.user_hkey is not null and s.diff_hkey <> t.diff_hkey then 'INACTIVE'  -- Flag old records as inactive
            else 'ACTIVE'  -- Mark new or unchanged records as active
        end as record_status,
        case
            when t.user_hkey is not null and s.diff_hkey <> t.diff_hkey then current_date  -- Set end_date for old records
            else null
        end as end_date
    from source_data s
    left join {{ this }} t  -- Join with the satellite table to check for existing records
        on s.user_hkey = t.user_hkey  -- Match based on the user_hkey (primary key)
)

-- Step 3: Insert new/updated records and flag old records as inactive
select
    user_hkey,
    user_name,
    email,
    birthdate,
    registration_date,
    address,
    phone_number,
    created_at,
    updated_at,
    record_status,
    end_date
from merge_data
