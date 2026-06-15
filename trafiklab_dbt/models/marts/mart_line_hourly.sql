select
    line,
    extract(hour from arrival_ts at time zone "Europe/Stockholm") as hour_of_day,
    count(*) as stop_events,
    round(avg(delay_minutes), 2) as avg_delay_minutes,
    round(avg(case when delay_seconds between {{ var('on_time_early_secs') }} and {{ var('on_time_late_secs') }} then 1.0 else 0 end) * 100, 1) as pct_on_time
from {{ref('stg_realtime_delays')}} 
where has_valid_arrival_time
group by line, hour_of_day