{{ config(
    materialized='incremental',
    unique_key='order_product_hkey'
) }}

with source_data as (
    select
        md5(TRIM(order_id::TEXT) || TRIM(product_id::TEXT)) as order_product_hkey,  -- Surrogate key for the relationship
        md5(TRIM(order_id::TEXT)) as order_hkey,                                                    -- Business key from orders
        md5(TRIM(product_id::TEXT)) as product_hkey,                                                  -- Business key from products
        created_at as load_timestamp,                                -- Load timestamp
        'streams.user_order_activity_stream' as record_source        -- Source of the record
    from {{ source('streams', 'user_order_activity_stream') }}     -- Source table (user-product link)
    {% if is_incremental() %}
    -- Incremental load logic: Only load new or updated records
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- Use a very early date if the table is empty
    )
    {% endif %}
)

select
    order_product_hkey,
    order_hkey,
    product_hkey,
    load_timestamp,
    record_source
from source_data