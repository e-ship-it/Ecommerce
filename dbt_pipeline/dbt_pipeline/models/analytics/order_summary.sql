{{ config(
    materialized='table',
    schema = 'mart'
) }}

WITH user_info AS (
    SELECT DIM_USER_ID,
        user_id,
        user_name,
        email,
        address,
        user_hkey
    FROM {{ ref('dim_user') }}
),

order_info AS (SELECT 
        order_id,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        order_hkey FROM
        (SELECT 
        ho.order_id,
        so.order_number,
        so.order_dow,
        so.order_hour_of_day,
        so.days_since_prior_order,
        ho.order_hkey,
        ROW_NUMBER() OVER (PARTITION BY so.order_hkey ORDER BY so.load_timestamp) as rn
    FROM {{ ref('hub_order') }} ho
    LEFT JOIN {{ ref('satellite_orders') }} so ON ho.order_hkey = so.order_hkey) t where rn =1 
),

user_order_link AS (
    SELECT 
        lou.order_hkey,
        lou.user_hkey
    FROM {{ ref('link_order_user') }} lou
),

activity_stream AS (
    SELECT 
        order_product_hkey,
        reordered,
        add_to_cart_order
    FROM {{ ref('satellite_order_user_order_activity_stream') }}
),

order_product_link AS (
    SELECT 
        order_product_hkey,
        order_hkey,
        product_hkey
    FROM {{ ref('link_order_product') }}
),

product_info AS( 
    SELECT product_hkey,
        product_id,
        product_name,
        department,
        aisle
    FROM {{ ref('dim_product') }} dp
),

final_summary AS (
    SELECT 
        ui.user_id,
        ui.user_name,
        oi.order_id,
        oi.order_number,
        oi.order_dow,
        oi.order_hour_of_day,
        oi.days_since_prior_order,
        pi.product_name,
        pi.department,
        pi.aisle,
        ast.reordered,
        ast.add_to_cart_order
    FROM user_order_link uol
    JOIN user_info ui ON uol.user_hkey = ui.user_hkey
    JOIN order_info oi ON uol.order_hkey = oi.order_hkey
    JOIN order_product_link opl ON opl.order_hkey = oi.order_hkey
    JOIN product_info pi ON pi.product_hkey = opl.product_hkey
    JOIN activity_stream ast ON ast.order_product_hkey = opl.order_product_hkey
)

SELECT * FROM final_summary