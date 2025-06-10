-- models/staging/link_user_product.sql
{{ config(
    materialized='incremental',
    unique_key='aisle_department_hkey'
) }}
--The Link table representing the relationship between users and products from the user_order_activity_stream.
with source_data as (
    select
        md5(aisle_id::TEXT || department_id::TEXT) as aisle_department_hkey,   -- Surrogate hash key for the relationship
        aisle_id,                                                    -- Business key from the order table
        department_id,                                                  -- Business key from the product table
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
    aisle_department_hkey,
    aisle_id,
    department_id,
    load_timestamp,
    record_source
from source_data