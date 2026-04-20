with source as (
    select *
    from {{ source('silver', 'ev_stations') }}
    where data_quality_error = false
      and is_current = true
)

select
    -- station identifiers
    station_id,
    uuid,
    operator_id,
    operator_name,
    usage_type_id,
    status_type_id,
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

    -- connection
    connection_id,
    connection_type_id,
    connection_type_name,
    connection_status_type_id,
    level_id,
    level_description,
    amps,
    voltage,
    power_kw,
    current_type_id,
    current_type_description,
    quantity,
    is_operational,

    -- metadata
    related_url,
    date_created,
    date_last_verified,
    date_last_status_update,
    valid_from,
    valid_to,
    is_current

from source