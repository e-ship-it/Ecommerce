{% snapshot satellite_product %}
{{ config(
    target_schema='staging',
    unique_key='product_hkey',
    strategy='check',
    check_cols=['product_name', 'aisle_id', 'department_id', 'updated_at']
) }}

--The Satellite table for product data will track changes in product details like product_name, aisle_id, department_id, etc.
select
    md5(product_id::TEXT) as product_hkey,  -- Surrogate hash key for product_id
    product_name,
    aisle_id,
    department_id,
    created_at,
    updated_at
from {{ source('batch', 'products') }}  -- Reference source table
{% endsnapshot %}
