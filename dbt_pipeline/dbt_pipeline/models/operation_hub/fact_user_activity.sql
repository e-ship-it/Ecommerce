/*WITH user_activity AS (
    -- Join hub_users, link_order_user, and satellite_order_user_order_activity_stream to get user activity data
    SELECT
        u.user_hkey,
        o.order_hkey,
        o.order_number,
        o.created_at AS activity_timestamp,
        o.add_to_cart_order,
        o.add_to_cart_order AS add_to_cart_time,
        o.reordered,
        o.add_to_cart_order AS remove_from_cart_order,  -- To be refined later for actual removal logic
        CASE
            WHEN o.add_to_cart_order IS NOT NULL THEN 'add_to_cart'
            WHEN o.reordered = 1 THEN 'reorder'
            ELSE 'other' 
        END AS activity_type
    FROM {{ ref('hub_users') }} u
    JOIN {{ ref('link_order_user') }} l ON u.user_hkey = l.user_hkey
    JOIN {{ ref('satellite_order_user_order_activity_stream') }} o ON l.user_order_hkey = o.user_order_hkey
),
user_activity_clean AS (
    -- Apply any cleaning logic needed, e.g., handle null values, filter out incomplete activities
    SELECT
        user_hkey,
        order_hkey,
        activity_timestamp,
        add_to_cart_order,
        add_to_cart_time,
        reordered,
        remove_from_cart_order,
        activity_type
    FROM user_activity
    WHERE add_to_cart_order IS NOT NULL OR reordered = 1
)

-- Final Fact table
SELECT
    -- Surrogate key generation for the fact table
    GENERATE_SERIES(1, 10000000) AS fact_user_activity_hkey, -- Simplified surrogate key, dbt will generate a more advanced method.
    user_hkey,
    order_hkey,
    add_to_cart_order,
    add_to_cart_time,
    reordered,
    remove_from_cart_order,
    activity_type,
    activity_timestamp,
    CURRENT_TIMESTAMP AS load_timestamp
FROM user_activity_clean;
+/