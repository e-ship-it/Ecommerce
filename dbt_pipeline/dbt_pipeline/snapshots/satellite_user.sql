{% snapshot satellite_user %}
{{ config(
    target_schema='staging',
    unique_key='user_hkey',  
    strategy='check',
    check_cols=['user_name', 'email', 'address', 'phone_number', 'birthdate']    
) }}


select
    md5(user_id::TEXT) as user_hkey,  -- Surrogate hash key for user_id
    TRIM(name) as user_name,
    TRIM(email) as email,
    birthdate,
    registration_date,
    TRIM(address) as address,
    TRIM(phone_number) as phone_number,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as load_timestamp
from {{ source('batch','users') }}  -- Reference source table
{% endsnapshot %}
