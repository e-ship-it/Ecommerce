{{ config(materialized='incremental', unique_key='user_order_hkey') }}

with link as (
    select user_order_hkey, user_hkey, order_hkey, load_timestamp
    from {{ ref('link_order_user') }}
),

with hub_order as (
    select *
    from {{ ref('hub_orders') }}
),

with hub_user as (
    select *
    from {{ ref('hub_users') }}
),

user_sat as (
    select user_hkey, name, email -- any other attributes
    from {{ ref('satellite_user') }}
    where load_timestamp = (
        select max(load_timestamp) from {{ ref('satellite_user') }} s2 where s2.user_hkey = satellite_user.user_hkey
    )
),

order_sat as (
    select order_hkey, order_number, order_dow, days_since_prior_order -- order details
    from {{ ref('satellite_order') }}
    where load_timestamp = (
        select max(load_timestamp) from {{ ref('satellite_order') }} s2 where s2.order_hkey = satellite_order.order_hkey
    )
)

select
    user_id,
    order_id,
    u.name,
    l.order_hkey,
    o.order_number,
    o.order_dow,
    o.days_since_prior_order,
    o.load_timestamp
from link l
left join user_sat u on l.user_hkey = u.user_hkey
left join order_sat o on l.order_hkey = o.order_hkey
join hub_user h1 on 1.user_hkey = h1.user_hkey
join hub_order h2 on 1.order_hkey = h2.order_hkey

{% if is_incremental() %}
where s.load_timestamp > coalesce((select max(load_timestamp) from {{ this }}), '1900-01-01')
{% endif %}
