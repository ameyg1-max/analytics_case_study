  select
  distinct
    data:"time"::timestamp                   as observation_time,
    data:"city"."id"::int                    as city_id,
    data:"city"."name"::string               as city_name,
    data:"city"."country"::string            as country,
    data:"city"."coord"."lat"::float         as city_latitude,
    data:"city"."coord"."lon"::float         as city_longitude,
    data:"clouds"."all"::int                 as clouds,
    (data:"main"."temp"::numeric(6,2)) - 273.15     as temp_avg,
    (data:"main"."temp_min"::numeric(6,2)) - 273.15 as temp_min,
    (data:"main"."temp_max"::numeric(6,2)) - 273.15 as temp_max,
    data:"weather"[0]."main"::string         as weather,
    data:"weather"[0]."description"::string  as weather_desc,
    data:"weather"[0]."icon"::string         as weather_icon,
    data:"wind"."deg"::float                 as wind_dir,
    data:"wind"."speed"::float               as wind_speed,
    {{ dbt_utils.generate_surrogate_key(['observation_time','city_id','city_name','country','city_latitude','city_longitude','clouds','temp_avg','temp_min','temp_max','weather','weather_desc','weather_icon','wind_dir','wind_speed'])}} as weather_id
  from 
    {{ source('weather', 'json_weather_data') }}
  where 
    city_id = 5128638