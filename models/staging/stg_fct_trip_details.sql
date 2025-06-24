-- source ctes
with trips as 
(
    select * from {{ ref('lnd_citibike_trips') }}
),
weather as (
    select * from {{ ref('lnd_weather_data') }}
),
trip_calculation as (
select 
trips.trip_id,
to_date(trips.start_time) as start_date,
to_date(trips.stop_time) as stop_date,
to_time(trips.start_time) as start_time,
to_time(trips.stop_time) as stop_time,
to_date(weather.observation_time) as observation_date,
to_time(weather.observation_time) as observation_time,
ifnull(trips.start_station_id,-1) as start_station_id,
ifnull(trips.end_station_id,-1) as end_station_id,
trips.start_station_name,
trips.end_station_name,
trips.gender,
trips.start_station_latitude,
trips.start_station_longitude,
trips.end_station_latitude,
trips.end_station_longitude,
ifnull(trips.bike_id,-1) as bike_id,
ifnull(trips.birth_year,-1) as birth_year,
ifnull(trips.usertype,'Unknown') as usertype,
ifnull(weather.weather,'Unknown') as weather_condition,
ifnull(weather.weather_desc,'Unknown') as weather_condition_description,
weather.wind_dir,
weather.wind_speed,
weather.temp_avg,
weather.temp_max,
weather.temp_min,
trips.trip_duration,
{{ dbt_utils.haversine_distance(
    lat1="start_station_latitude",
    lon1="start_station_longitude",
    lat2="end_station_latitude",
    lon2="end_station_longitude",
    unit='km') }} as trip_distance,
1 as trip_count,
 from trips
 inner join weather on to_date(trips.start_time)=to_date(weather.observation_time) and to_time(weather.observation_time) between to_time(trips.start_time)
and to_time(trips.stop_time) )
 select 
  ifnull(to_number(to_varchar(start_date,'yyyymmdd')),-1) as dim_start_date_key,
  ifnull(to_number(to_varchar(stop_date,'yyyymmdd')),-1) as dim_stop_date_key,
  ifnull((hour(start_time)*10000)+(minute(start_time)*100)+(second(start_time)),-1) as dim_start_time_key,
ifnull((hour(stop_time)*10000)+(minute(stop_time)*100)+(second(stop_time)),-1) as dim_stop_time_key,
 case when start_station_id = -1 then '-1' else {{dbt_utils.generate_surrogate_key(['start_station_id'])}} end as dim_start_station_key,
case when end_station_id = -1 then '-1' else {{ dbt_utils.generate_surrogate_key(['end_station_id'])}} end as dim_end_station_key,
 case when birth_year = -1 and gender =0 then '-1' else {{ dbt_utils.generate_surrogate_key(['birth_year','gender'])}} end as dim_rider_detail_key,
 case when usertype = 'Unknown' then '-1' else {{ dbt_utils.generate_surrogate_key(['usertype'])}} end as dim_usertype_key,
case when bike_id = -1 then '-1' else {{ dbt_utils.generate_surrogate_key( ['bike_id'])}} end as dim_bike_key,
 {{ dbt_utils.generate_surrogate_key(['weather_condition','weather_condition_description'])}} as dim_weather_description_key,
 wind_dir,
 wind_speed,
 temp_avg,
 temp_max,
 temp_min,
 start_station_latitude,
 start_station_longitude,
 end_station_latitude,
 end_station_longitude,
 trip_duration,
 trip_distance,
 trip_count
 from trip_calculation