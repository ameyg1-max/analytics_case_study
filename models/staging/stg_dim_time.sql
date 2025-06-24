with time_spine as (
    {{
        dbt_utils.date_spine(
            datepart="seconds",
            start_date="to_timestamp_ntz('01/01/1900 00:00:00')",
            end_date="to_timestamp_ntz('01/01/1900 23:59:59')"
        )
    }}
),
 final as(
select 
    (hour(date_seconds)*10000)+(minute(date_seconds)*100)+(second(date_seconds)) as time_key,
    cast (date_seconds as time) as full_time,
    cast (cast (date_seconds as time ) as varchar) as time_full_24_hr_text,
    cast(time_from_parts((hour(date_seconds)%12),minute(date_seconds),second(date_seconds)) as varchar) as time_full_12_hr_text,
    hour(date_seconds) as hour_24_number,
    to_varchar(hour(date_seconds),'00') as hour_24_short_text,
    concat(to_varchar(hour(date_seconds),'00'),':00') as hour_24_minute_text,
    concat(to_varchar(hour(date_seconds),'00'),':00',':00') as hour_24_full_text,
    hour(date_seconds)%12 as hour_12_number,
    to_varchar(hour(date_seconds)%12,'00') as hour_12_short_text,
    concat(to_varchar(hour(date_seconds)%12,'00'),':00') as hour_12_minute_text,
    concat(to_varchar(hour(date_seconds)%12,'00'),':00',':00') as hour_12_full_text,
    hour(date_seconds)/12 as am_pm_code,
    case when am_pm_code < 12 then 'AM' else 'PM' end as am_pm_indicator,
    case when full_time between '00:00:00' and '05:00:00' then 'Midnight'
    when full_time between '05:00:00' and '08:00:00' then 'Early Morning'
    when full_time between '08:00:00' and '12:00:00' then 'Morning'
    when full_time between '12:00:00' and '17:00:00' then ' Afternoon'
    when full_time between '17:00:00' and '20:00:00' then ' Evening'
    else 'Late Night' end as day_time_description,
    minute(date_seconds) as minute_number,
    (hour(date_seconds)*100) + minute(date_seconds) as minute_code,
    to_varchar(minute(date_seconds),'00') as minute_short_text,
    concat(to_varchar(hour(date_seconds),'00'),to_varchar(minute(date_seconds),':00'),':00') as minute_24_full_text,
    concat(to_varchar(hour(date_seconds)%12,'00'),to_varchar(minute(date_seconds),':00'),':00') as minute_12_full_text,
    cast((minute(date_seconds)/30) as int) as half_hour_number,
    (hour(date_seconds)*100)+(cast((minute(date_seconds)/30) as int)*30) as half_hour_code,
    to_varchar((cast((minute(date_seconds)/30) as int)*30),'00') half_hour_short_text,
    concat(to_varchar(hour(date_seconds),'00'),to_varchar((cast((minute(date_seconds)/30) as int)*30),':00'),':00') as half_hour_24_full_text,
    concat(to_varchar(hour(date_seconds)%12,'00'),to_varchar((cast((minute(date_seconds)/30) as int)*30),':00'),':00') as half_hour_12_full_text,
    second(date_seconds) as second_number,
    to_varchar(second(date_seconds),'00') as second_short_text
 from time_spine)
 select * from final