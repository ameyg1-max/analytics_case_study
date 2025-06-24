select 
distinct
{{ dbt_utils.generate_surrogate_key(['birth_year','gender'])}} as dim_rider_detail_key,
birth_year,
case when gender =0 then 'Unknown' 
 when gender = 1 then 'Male'
 when gender =2 then 'Female' end as gender_description
from {{ ref('lnd_citibike_trips') }}
union
select 
'-1' as dim_rider_detail_key,
-1 as birth_year,
'Unknown' as gender_description
