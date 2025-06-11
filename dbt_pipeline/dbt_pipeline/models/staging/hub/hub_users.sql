{{ config(
    materialized='incremental',
    unique_key='user_hkey'
) }}
-- Incremental load to only process new or updated records
-- The unique key to identify each record

with source_data as (
    select
    md5((user_id)::TEXT) as user_hkey, 
    user_id,
    created_at as load_timestamp,
    'batch.users' as record_source
    from {{source('batch','users')}}
    {% if is_incremental() %}
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- a default very early date if the table is empty)
)
{% endif %}
)

select
    user_hkey,
    user_id,
    load_timestamp,
    record_source
from source_data
