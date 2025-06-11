{{ config(
    materialized='incremental',
    unique_key='product_hkey',
    schema='opts_hub'
) }}

{% if is_incremental() %}

with latest_sat as (
    select
        l.product_hkey,
        h.product_id,
        s.product_name,
        s.created_at,
        s.updated_at,
        l.aisle_department_product_hkey,
        l.aisle_hkey,
        l.department_hkey,
        s.load_timestamp,
        row_number() over (partition by l.product_hkey order by s.load_timestamp desc) as rn
    from {{ ref('link_aisle_department_product') }} l
    join {{ ref('satellite_product') }} s on s.aisle_department_product_hkey = l.aisle_department_product_hkey
    join {{ ref('hub_product') }} h on l.product_hkey = h.product_hkey
),

current_records as (
    select * from latest_sat where rn = 1
),

with_dimensions as (
    select
        w.product_hkey,
        w.product_id,
        w.product_name,
        w.created_at,
        w.updated_at,
        a.aisle_id,
        s1.aisle,
        d.department_id,
        s2.department,
        w.load_timestamp,
        row_number() over (partition by w.product_hkey order by w.load_timestamp desc) as rn
    from current_records w
    left join {{ ref('hub_aisles') }} a on w.aisle_hkey = a.aisle_hkey
    left join {{ ref('hub_departments') }} d on w.department_hkey = d.department_hkey
    left join {{ source('batch', 'aisles') }} s1 on a.aisle_id = s1.aisle_id
    left join {{ source('batch', 'departments') }} s2 on d.department_id = s2.department_id
),

current_joined as (
    select * from with_dimensions where rn = 1
),

existing_dim as (
    select * from {{ this }}
),

changes as (
    select
        c.product_hkey,
        c.product_id,
        c.product_name,
        c.aisle_id,
        c.aisle,
        c.department_id,
        c.department,
        c.created_at,
        c.updated_at,
        c.load_timestamp as valid_from,
        cast('9999-12-31' as timestamp) as valid_to
    from current_joined c
    left join existing_dim e
      on c.product_hkey = e.product_hkey and e.valid_to = cast('9999-12-31' as timestamp)
    where
      e.product_hkey is null
      or (
        c.product_name   is distinct from e.product_name
        or c.aisle_id     is distinct from e.aisle_id
        or c.department_id is distinct from e.department_id
        or c.created_at   is distinct from e.created_at
        or c.updated_at   is distinct from e.updated_at
      )
),

expired_old as (
    select e.*
    from existing_dim e
    join changes c on e.product_hkey = c.product_hkey
    where e.valid_to = cast('9999-12-31' as timestamp)
),

expired_old_update as (
    select
        e.product_hkey,
        e.product_id,
        e.product_name,
        e.aisle_id,
        e.aisle,
        e.department_id,
        e.department,
        e.created_at,
        e.updated_at,
        e.valid_from,
        c.valid_from - interval '1 second' as valid_to
    from expired_old e
    join changes c on e.product_hkey = c.product_hkey
)

select * from expired_old_update

union all

select
    product_hkey,
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    created_at,
    updated_at,
    valid_from,
    valid_to
from changes
where valid_from > (
    select coalesce(max(valid_from), '1900-01-01'::timestamp) from {{ this }}
)

{% else %}

with latest_sat as (
    select
        l.product_hkey,
        h.product_id,
        s.product_name,
        s.created_at,
        s.updated_at,
        l.aisle_department_product_hkey,
        l.aisle_hkey,
        l.department_hkey,
        s.load_timestamp,
        row_number() over (partition by l.product_hkey order by s.load_timestamp desc) as rn
    from {{ ref('link_aisle_department_product') }} l
    join {{ ref('satellite_product') }} s on s.aisle_department_product_hkey = l.aisle_department_product_hkey
    join {{ ref('hub_product') }} h on l.product_hkey = h.product_hkey
),

current_records as (
    select * from latest_sat where rn = 1
),

with_dimensions as (
    select
        w.product_hkey,
        w.product_id,
        w.product_name,
        w.created_at,
        w.updated_at,
        a.aisle_id,
        s1.aisle,
        d.department_id,
        s2.department,
        w.load_timestamp,
        row_number() over (partition by w.product_hkey order by w.load_timestamp desc) as rn
    from current_records w
    left join {{ ref('hub_aisles') }} a on w.aisle_hkey = a.aisle_hkey
    left join {{ ref('hub_departments') }} d on w.department_hkey = d.department_hkey
    left join {{ source('batch', 'aisles') }} s1 on a.aisle_id = s1.aisle_id
    left join {{ source('batch', 'departments') }} s2 on d.department_id = s2.department_id
)

select
    product_hkey,
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    created_at,
    updated_at,
    load_timestamp as valid_from,
    cast('9999-12-31' as timestamp) as valid_to
from with_dimensions

{% endif %}
