with source as (
    select *
    from {{ source('silver', 'weather') }}
)

select
    -- zone join keys
    lat_round,
    lon_round,

    -- temperature (both units — risk model uses fahrenheit)
    temperature_c,
    temperature_f,

    -- conditions
    condition,
    condition_description,
    wind_speed,
    humidity,

    -- metadata
    ingested_at,
    processed_at

from source