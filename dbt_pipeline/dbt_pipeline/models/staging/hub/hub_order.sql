{{ config(
    materialized='incremental',
    unique_key='order_hkey'
) }}

with source_data as (
    select
        md5(order_id::TEXT) as order_hkey,   -- Surrogate hash key
        order_id,                              -- Business key
        created_at as load_timestamp,            -- Load timestamp
        'streams.orders' as record_source        -- Source of the record
    from {{ source('streams', 'orders') }}    -- Source table
    {% if is_incremental() %}
    -- Incremental load logic: Only load new or updated records
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- Use a very early date if the table is empty
    )
    {% endif %}
)

select
    order_hkey,
    order_id,
    load_timestamp,
    record_source
from source_data