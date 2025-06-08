{{ config(
    materialized='incremental',  -- Only insert new records
    schema='staging',  -- Keep raw tables in the 'staging' schema
    unique_key='user_hkey'  -- The unique key for your hub (typically a surrogate key)
) }}

with source_data as (
    select
    md5((user_id)::TEXT) as user_hkey, 
    user_id,
    created_at as load_timestamp,
    "batch.users" as record_source
    from {{source('batch','users')}}
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- a default very early date if the table is empty)
)
)

select
    user_hkey,
    user_id,
    load_timestamp,
    record_source
from source_data
