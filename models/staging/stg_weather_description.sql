select distinct
{{ dbt_utils.generate_surrogate_key(['weather','weather_desc'])}} as dim_weather_description_key,
weather as weather_condition,
weather_desc as weather_condition_description
from {{ ref('lnd_weather_data') }}
union
select
'-1' as dim_weather_desription_key,
'Unknown' as weather_condition,
'Unknown' as weather_condition_description