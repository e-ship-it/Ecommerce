{% snapshot satellite_order %}
{{ config(
    target_schema='staging',
    unique_key='order_hkey',
    strategy='check',
    check_cols=['order_number', 'order_dow', 'days_since_prior_order']
) }}
--The Satellite table for orders stores the historical changes for order-related fields like order_number, order_dow, and days_since_prior_order.
select
    md5(order_id::TEXT) as order_hkey,  -- Surrogate hash key for order_id
    order_number,
    order_dow,
    days_since_prior_order,
    created_at,
    updated_at
from {{ source('streams', 'orders') }}  -- Reference source table
{% endsnapshot %}
