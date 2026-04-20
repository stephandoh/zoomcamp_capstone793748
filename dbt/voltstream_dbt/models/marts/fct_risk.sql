{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Risk score model — translates weather severity and station
    characteristics into a 0-100 risk score per station + connection.

    Scoring logic (from project brief):
      weather_score : +40 if Thunderstorm or Snow
                      +20 if Rain
                      +10 otherwise (base)
      temp_score    : +20 if temperature_f > 95 (extreme heat)
                      +20 if temperature_f < 20 (extreme cold)
                      +0  otherwise
      wind_score    : +20 if wind_speed > 20 mph
                      +0  otherwise
      power_score   : +10 if power_kw > 50 (DC fast charger, draws more grid load)
                      +0  otherwise

    Max possible score = 40 + 20 + 20 + 10 = 90 (capped at 100)

    energy_rate is defined as a dbt variable with a default of 0.35 ($/kWh)
    Override at run time: dbt run --vars '{"energy_rate": 0.42}'
*/

with ev as (
    select * from {{ ref('stg_ev') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        ev.station_id,
        ev.connection_id,
        ev.title,
        ev.address_line1,
        ev.town,
        ev.state_or_province,
        ev.postcode,
        ev.country_iso_code,
        ev.latitude,
        ev.longitude,
        ev.lat_round,
        ev.lon_round,
        ev.operator_name,
        ev.power_kw,
        ev.connection_type_name,
        ev.level_id,
        ev.is_operational,
        ev.date_last_status_update,
        weather.temperature_c,
        weather.temperature_f,
        weather.condition,
        weather.condition_description,
        weather.wind_speed,
        weather.humidity,
        -- surrogate key to link to dim_weather
        md5(
            cast(ev.lat_round as varchar) || ',' || cast(ev.lon_round as varchar)
        ) as weather_zone_id
    from ev
    left join weather
        on ev.lat_round  = weather.lat_round
        and ev.lon_round = weather.lon_round
),

scored as (
    select
        *,

        -- weather condition score
        case
            when condition in ('Thunderstorm', 'Snow') then 40
            when condition = 'Rain'                    then 20
            else 10
        end as weather_score,

        -- temperature score (fahrenheit thresholds from brief)
        case
            when temperature_f > 95 or temperature_f < 20 then 20
            else 0
        end as temp_score,

        -- wind score
        case
            when wind_speed > 20 then 20
            else 0
        end as wind_score,

        -- power score — DC fast chargers draw more grid load
        case
            when power_kw > 50 then 10
            else 0
        end as power_score

    from joined
),

final as (
    select
        *,

        -- total risk score capped at 100
        least(
            weather_score + temp_score + wind_score + power_score,
            100
        ) as risk_score,

        -- risk band for dashboard filtering
        case
            when least(weather_score + temp_score + wind_score + power_score, 100) >= 60
                then 'High'
            when least(weather_score + temp_score + wind_score + power_score, 100) >= 30
                then 'Medium'
            else 'Low'
        end as risk_band,

        -- estimated revenue at risk per hour for offline stations
        -- energy_rate defaults to $0.35/kWh, configurable via dbt variable
        case
            when is_operational = false or is_operational is null
            then round(
                coalesce(power_kw, 0) * {{ var('energy_rate', 0.35) }},
                2
            )
            else 0
        end as est_revenue_at_risk,

        -- pipeline timestamp
        current_timestamp as scored_at

    from scored
)

select * from final