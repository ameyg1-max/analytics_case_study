with 
 stations as (
select
distinct
start_station_id as station_id,
start_station_name as station_name
from
{{ ref('lnd_citibike_trips') }}
union 
select 
distinct
end_station_id,
end_station_name
from
{{ ref('lnd_citibike_trips') }})
select 
{{ dbt_utils.generate_surrogate_key(['station_id'])}} as dim_station_key,
station_id,
station_name,
from
stations
where station_id is not null
union 
select
'-1' as dim_station_key,
-1 as station_id,
'Unknown' as station_name,