select 
to_number(to_varchar(date_day,'yyyymmdd')) as dim_date_key,
*
from {{ ref('stg_dim_date') }}