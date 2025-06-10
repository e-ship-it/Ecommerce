{% snapshot satellite_order_user_order_activity_stream %}
{{ config(
    target_schema='staging',
    unique_key='order_product_hkey',
    strategy='check',
    check_cols=['add_to_cart_order', 'reordered']
) }}
--The Satellite table for orders stores the historical changes for order-related fields like order_number, order_dow, and days_since_prior_order.
select
    md5(TRIM(order_id::TEXT) || TRIM(product_id::TEXT)) as order_product_hkey,  -- Surrogate hash key for order_id
    add_to_cart_order,
    reordered,
    created_at,
    updated_at
from {{ source('streams', 'user_order_activity_stream') }}  -- Reference source table
{% endsnapshot %}
