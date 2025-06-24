select 
distinct
{{ dbt_utils.generate_surrogate_key(['bike_id'])}} as dim_bike_key,
bike_id
from {{ ref('lnd_citibike_trips') }}
union 
select
'-1' as dim_bike_key,
-1 as bike_id