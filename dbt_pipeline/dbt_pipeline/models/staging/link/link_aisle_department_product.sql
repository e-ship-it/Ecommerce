-- models/staging/link_user_product.sql
{{ config(
    materialized='incremental',
    unique_key='aisle_department_product_hkey'
) }}
--The Link table representing the relationship between users and products from the user_order_activity_stream.
with source_data as (
    select
        md5(TRIM(aisle_id::TEXT) || TRIM(department_id::TEXT) || TRIM(product_id::TEXT)) as aisle_department_product_hkey,   -- Surrogate hash key for the relationship
        md5(TRIM(product_id::TEXT)) as product_hkey,
        md5(TRIM(aisle_id::TEXT)) as aisle_hkey,                                                    -- Business key from the order table
        md5(TRIM(department_id::TEXT)) as department_hkey,                                                  -- Business key from the product table
        created_at as load_timestamp,                                -- Load timestamp
        'batch.products' as record_source        -- Source of the record
    from {{ source('batch', 'products') }}     -- Source table (user-product link)
    {% if is_incremental() %}
    -- Incremental load logic: Only load new or updated records
    where created_at > coalesce(
        (select max(load_timestamp) from {{ this }}),
        '1900-01-01'::timestamp  -- Use a very early date if the table is empty
    )
    {% endif %}
)

select
    aisle_department_product_hkey,
    product_hkey,
    aisle_hkey,
    department_hkey,
    load_timestamp,
    record_source
from source_data