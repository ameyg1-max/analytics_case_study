with 
 stations as (
select
distinct
start_station_id as station_id,
start_station_name as station_name,
start_station_latitude as station_latitude,
start_station_longitude as station_longitude
from
{{ ref('lnd_citibike_trips') }}
union 
select 
distinct
end_station_id,
end_station_name,
end_station_latitude,
end_station_longitude,
from
{{ ref('lnd_citibike_trips') }})
select 
{{ dbt_utils.generate_surrogate_key(['station_id','station_name','station_latitude','station_longitude'])}} as dim_station_key,
station_id,
station_name,
station_latitude,
station_longitude
from
stations
where station_id is not null
union 
select
'-1' as dim_station_key,
-1 as station_id,
'Unknown' as station_name,
-1 as station_latitude,
-1 as station_longitude