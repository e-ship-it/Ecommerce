{{ config(
    materialized='incremental',
    unique_key='aisle_hkey'
) }}

with source_data as (
    select
        md5(aisle_id::TEXT) as aisle_hkey,   -- Surrogate hash key
        aisle_id,                              -- Business key
        created_at as load_timestamp,            -- Load timestamp
        'batch.aisles' as record_source        -- Source of the record
    from {{ source('batch', 'aisles') }}    -- Source table
    {% if is_incremental() %}
    -- Incremental load logic: Only load new or updated records
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- Use a very early date if the table is empty
    )
    {% endif %}
)

select
    aisle_hkey,
    aisle_id,
    load_timestamp,
    record_source
from source_data