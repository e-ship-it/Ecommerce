{% snapshot satellite_order %}
{{ config(
    target_schema='staging',
    unique_key='user_order_hkey',
    strategy='check',
    check_cols=['eval_set', 'order_number', 'order_dow', 'order_hour_of_day', 'days_since_prior_order']
) }}
--The Satellite table for orders stores the historical changes for order-related fields like order_number, order_dow, and days_since_prior_order.
select
    md5(TRIM(order_id::TEXT) || TRIM(user_id::TEXT)) as user_order_hkey,  -- Surrogate hash key for order_id
    TRIM(eval_set) as eval_set,
    order_number,
    order_dow,
    order_hour_of_day,
    days_since_prior_order,
    created_at,
    updated_at
from {{ source('streams', 'orders') }}  -- Reference source table
{% endsnapshot %}