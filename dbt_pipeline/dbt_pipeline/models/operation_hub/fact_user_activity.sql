{{ config(
    materialized= 'incremental',
    unique_key= ['order_id', 'dim_user_id'],
    schema= 'opts_hub',
    post_hook=[
        "
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE constraint_name = 'fact_user_activity_pk'
                  AND table_name = 'fact_user_activity'
            ) THEN
                ALTER TABLE {{ this }} ADD CONSTRAINT fact_order_pk PRIMARY KEY (order_id, dim_user_id);
            END IF;
        END
        $$;
        "
    ]
) }}

WITH user_activity AS (
    SELECT
        d.DIM_USER_ID,
        d.DIM_PRODUCT_ID,
        d.product_name,
        h.order_id,
        d.order_number,
        o.add_to_cart_order,
        o.created_at AS add_to_cart_time,
        o.reordered,
        CASE
            WHEN o.add_to_cart_order IS NOT NULL THEN 'add_to_cart'
            WHEN o.reordered = 1 THEN 'reorder'
            ELSE 'other' 
        END AS activity_type,
        ROW_NUMBER() OVER (PARTITION BY h.order_id,DIM_USER_ID ORDER BY o.load_timestamp) as rn,
        o.load_timestamp
    FROM {{ ref('satellite_order_user_order_activity_stream') }} o
    JOIN {{ ref('link_order_product') }} l ON o.order_product_hkey = l.order_product_hkey
    LEFT JOIN {{ ref('link_order_user')}} l1 ON l1.order_hkey = l.order_hkey
    JOIN {{ ref('hub_order') }} h ON h.order_hkey = l1.order_hkey
    LEFT JOIN {{ ref('fact_order') }} d ON d.order_id = h.order_id
),

latest_data AS (
    SELECT * FROM user_activity WHERE rn =1
),

user_activity_clean AS (
    -- Apply any cleaning logic needed, e.g., handle null values, filter out incomplete activities
    SELECT
        DIM_USER_ID,
        DIM_PRODUCT_ID,
        product_name,
        order_id,
        order_number,
        add_to_cart_order,
        add_to_cart_time,
        reordered,
        activity_type
    FROM latest_data
    WHERE add_to_cart_order IS NOT NULL OR reordered = 1
)

-- Final Fact table
{% if is_incremental() %}
SELECT
    -- Surrogate key generation for the fact table
    (COALESCE((SELECT MAX(fact_user_activity_id) FROM {{this}}), 0) + ROW_NUMBER() OVER (ORDER BY add_to_cart_time)) AS fact_user_activity_id,
    DIM_USER_ID,
    DIM_PRODUCT_ID,
    product_name,
    order_id,
    order_number,
    add_to_cart_order,
    add_to_cart_time,
    reordered,
    activity_type,
    CURRENT_TIMESTAMP AS load_timestamp
FROM user_activity_clean

{% else %}
SELECT
    (ROW_NUMBER() OVER (ORDER BY add_to_cart_time)) AS fact_user_activity_id,
    DIM_USER_ID,
    DIM_PRODUCT_ID,
    product_name,
    order_id,
    order_number,
    add_to_cart_order,
    add_to_cart_time,
    reordered,
    activity_type,
    CURRENT_TIMESTAMP AS load_timestamp
FROM user_activity_clean
{% endif %}