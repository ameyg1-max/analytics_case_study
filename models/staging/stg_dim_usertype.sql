select 
distinct 
{{ dbt_utils.generate_surrogate_key(['usertype'])}} as dim_usertype_key,
usertype
from {{ ref('lnd_citibike_trips') }} where usertype is not null
union
select
'-1' as dim_usertype_key,
'Unknown' as usertype