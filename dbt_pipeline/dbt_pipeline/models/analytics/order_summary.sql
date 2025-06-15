{{ config(
    materialized='table',
    schema = 'mart'
) }}

WITH user_info AS (
    SELECT 
        hu.user_id,
        su.user_name,
        su.email,
        su.address,
        hu.user_hkey
    FROM {{ ref('hub_users') }} hu
    LEFT JOIN {{ ref('satellite_user') }} su ON hu.user_hkey = su.user_hkey
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

product_info AS( SELECT 
        product_id,
        product_name,
        product_hkey,
        aisle_department_product_hkey FROM(
    SELECT 
        hp.product_id,
        sp.product_name,
        hp.product_hkey,
        sp.aisle_department_product_hkey,
        ROW_NUMBER() OVER (PARTITION BY sp.product_hkey ORDER BY sp.load_timestamp) as rn
    FROM {{ ref('hub_product') }} hp
    LEFT JOIN {{ ref('satellite_product') }} sp ON hp.product_hkey = sp.product_hkey) t where rn =1
),

product_mapping AS (
    SELECT 
        adp.product_hkey,
        adp.aisle_hkey,
        adp.department_hkey
    FROM {{ ref('link_aisle_department_product') }} adp
),

aisle_department_info AS (
    SELECT 
        ha.aisle_id,
        ha.aisle_hkey,
        hd.department_id,
        hd.department_hkey
    FROM {{ ref('hub_aisles') }} ha
    JOIN {{ ref('hub_departments') }} hd ON 1=1
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
        adi.department,
        ast.reordered,
        ast.add_to_cart_order
    FROM user_order_link uol
    JOIN user_info ui ON uol.user_hkey = ui.user_hkey
    JOIN order_info oi ON uol.order_hkey = oi.order_hkey
    JOIN order_product_link opl ON opl.order_hkey = oi.order_hkey
    JOIN product_info pi ON pi.product_hkey = opl.product_hkey
    JOIN activity_stream ast ON ast.order_product_hkey = opl.order_product_hkey
    JOIN product_mapping pm ON pm.product_hkey = pi.product_hkey
    JOIN aisle_department_info adi 
        ON adi.aisle_hkey = pm.aisle_hkey AND adi.department_hkey = pm.department_hkey
)

SELECT * FROM final_summary