select
distinct
{{ dbt_utils.generate_surrogate_key(['trip_duration','start_time','stop_time','start_station_id','start_station_name','start_station_latitude', 'start_station_longitude','end_station_id','end_station_name','end_station_latitude','end_station_longitude','bike_id','usertype','birth_year','gender'] )}} as trip_id,
trip_duration,  
start_time,              
stop_time,              
start_station_id,       
start_station_name,      
start_station_latitude, 
start_station_longitude,
end_station_id,       
end_station_name,        
end_station_latitude,   
end_station_longitude,  
bike_id,             
usertype,                
birth_year,              
gender                
from {{ source('citibike', 'trips') }}

