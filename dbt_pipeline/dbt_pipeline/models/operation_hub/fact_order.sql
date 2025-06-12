{{ config(
    materialized='incremental',
    unique_key='fact_order_id',
    schema= 'opts_hub'
) }}



WITH order_details AS (
    SELECT 
    DIM_USER_ID,
    DIM_PRODUCT_ID,
    order_id,
    order_number,
    order_created_at,
    order_dow,
    order_hour_of_day,
    days_since_prior_order,
    product_name,
    department FROM
    (SELECT
        d1.DIM_USER_ID,
        d2.DIM_PRODUCT_ID,
        h.order_id,
        o.order_number,
        o.created_at AS order_created_at,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order,
        d2.product_name,
        d2.department,
        ROW_NUMBER() OVER (PARTITION BY u.user_order_hkey ORDER BY o.load_timestamp) as rn
    FROM {{ ref('satellite_orders') }} o
    JOIN {{ ref('link_order_product') }} p ON o.order_hkey = p.order_hkey
    JOIN {{ ref('link_order_user') }} u ON o.order_hkey = u.order_hkey
    JOIN {{ ref('hub_order')}} h on h.order_hkey = o.order_hkey
    LEFT JOIN {{ ref('dim_user')}} d1 on d1.user_hkey = u.user_hkey
    LEFT JOin {{ ref('dim_product')}} d2 on d2.product_hkey= p.product_hkey
)t where rn =1 )

-- Final Fact table
    
{% if is_incremental() %}
SELECT
    (COALESCE((SELECT MAX(fact_order_id) FROM {{this}}), 0) + ROW_NUMBER() OVER (ORDER BY order_created_at)) AS fact_order_id,
    DIM_USER_ID,
    DIM_PRODUCT_ID,
    order_id,
    order_number,
    order_created_at,
    order_dow,
    order_hour_of_day,
    days_since_prior_order,
    product_name,
    department,
    CURRENT_TIMESTAMP AS load_timestamp
FROM order_details

{% else %}

SELECT
    (ROW_NUMBER() OVER (ORDER BY order_created_at)) AS fact_order_id,
    DIM_USER_ID,
    DIM_PRODUCT_ID,
    order_id,
    order_number,
    order_created_at,
    order_dow,
    order_hour_of_day,
    days_since_prior_order,
    product_name,
    department,
    CURRENT_TIMESTAMP AS load_timestamp
    FROM order_details
{% endif %}   