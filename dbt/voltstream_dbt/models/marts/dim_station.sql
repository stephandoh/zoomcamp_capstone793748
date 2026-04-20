{{
    config(materialized='table')
}}

with stations as (
    select * from {{ ref('stg_ev') }}
),

-- one row per station — keep most recently verified record
-- connection-level detail lives in the fact table
deduped as (
    select
        *,
        row_number() over (
            partition by station_id
            order by date_last_verified desc
        ) as rn
    from stations
)

select
    -- station identifiers
    station_id,
    uuid,
    operator_id,
    operator_name,
    usage_type_id,
    number_of_points,
    is_recently_verified,

    -- address
    title,
    address_line1,
    address_line2,
    town,
    state_or_province,
    postcode,
    country_id,
    country_iso_code,

    -- geo
    latitude,
    longitude,
    lat_round,
    lon_round,

    -- metadata
    related_url,
    date_created,
    date_last_verified

from deduped
where rn = 1