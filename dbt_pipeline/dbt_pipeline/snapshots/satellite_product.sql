{% snapshot satellite_product %}
{{ config(
    target_schema='staging',
    unique_key='aisle_department_product_hkey',
    strategy='check',
    check_cols=['product_name']
) }}

--The Satellite table for product data will track changes in product details like product_name, aisle_id, department_id, etc.
select
    md5(TRIM(aisle_id::TEXT) || TRIM(department_id::TEXT) || TRIM(product_id::TEXT)) as aisle_department_product_hkey,  -- Surrogate hash key for product_id
    TRIM(product_name) as product_name,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as load_timestamp
from {{ source('batch', 'products') }}  -- Reference source table
{% endsnapshot %}
