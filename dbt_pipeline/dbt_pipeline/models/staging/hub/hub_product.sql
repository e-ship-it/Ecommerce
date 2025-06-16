{{ config(
    materialized='incremental',
    unique_key='product_hkey'
) }}

with source_data as (
    select
        md5(product_id::TEXT) as product_hkey,   -- Surrogate hash key
        product_id,                              -- Business key
        created_at as load_timestamp,            -- Load timestamp
        'batch.products' as record_source        -- Source of the record
    from {{ source('batch', 'products') }}    -- Source table
    {% if is_incremental() %}
    -- Incremental load logic: Only load new or updated records
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- Use a very early date if the table is empty
    )
    {% endif %}
)

select
    product_hkey,
    product_id,
    load_timestamp,
    record_source
from source_data