{{ config(
    materialized='incremental',
    unique_key='user_order_hkey'
) }}
-- The Link table represents the relationship between two Hub tables (e.g., users and orders).
with source_data as (
    select
        md5(TRIM(order_id::TEXT) || TRIM(user_id::TEXT)) as user_order_hkey,   -- Surrogate hash key for the relationship
        md5(TRIM(order_id::TEXT)) as order_hkey,                                                -- Business key from the order table
        md5(TRIM(user_id::TEXT)) as user_hkey,                                                 -- Business key from the user table
        created_at as load_timestamp,                            -- Load timestamp
        'streams.orders' as record_source                        -- Source of the record
    from {{ source('streams', 'orders') }}                      -- Source table (orders)
    {% if is_incremental() %}
    -- Incremental load logic: Only load new or updated records
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- Use a very early date if the table is empty
    )
    {% endif %}
)

select
    user_order_hkey,
    order_hkey,
    user_hkey,
    load_timestamp,
    record_source
from source_data