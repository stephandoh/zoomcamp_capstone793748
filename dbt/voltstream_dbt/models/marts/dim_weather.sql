{{
    config(materialized='table')
}}

with weather as (
    select * from {{ ref('stg_weather') }}
),

-- deduplicate at model level — one row per zone
-- keeps most recently ingested reading per zone
deduped as (
    select
        *,
        row_number() over (
            partition by lat_round, lon_round
            order by ingested_at desc
        ) as rn
    from weather
)

select
    md5(cast(lat_round as varchar) || ',' || cast(lon_round as varchar)) as weather_zone_id,
    lat_round,
    lon_round,
    temperature_c,
    temperature_f,
    condition,
    condition_description,
    wind_speed,
    humidity,
    ingested_at,
    processed_at
from deduped
where rn = 1