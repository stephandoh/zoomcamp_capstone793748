{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['station_id', 'connection_id', 'scored_at']
    )
}}

/*
    Fact table — one row per station + connection + scoring run.
    Incremental materialisation means each dbt run appends new
    scored records rather than rewriting the entire table.
    This preserves the time-series history needed for the
    weather correlation and downtime trend charts in the dashboard.
*/

with risk as (
    select * from {{ ref('fct_risk') }}
)

select
    -- grain: one row per station + connection per pipeline run
    station_id,
    connection_id,
    weather_zone_id,

    -- status
    is_operational,
    risk_score,
    risk_band,
    est_revenue_at_risk,

    -- score components — useful for debugging and dashboard drill-through
    weather_score,
    temp_score,
    wind_score,
    power_score,

    -- weather snapshot at time of scoring
    condition,
    temperature_f,
    wind_speed,
    humidity,

    -- timestamps
    date_last_status_update,
    scored_at

from risk

{% if is_incremental() %}
    -- on incremental runs only add rows scored after the latest
    -- existing record in the table
    where scored_at > (select max(scored_at) from {{ this }})
{% endif %}