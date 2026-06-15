select
    max(arrival_ts) as newest_arrival_ts,
    min(arrival_ts) as oldest_arrival_ts,
    count(*) as total_records,
    count(distinct trip_id) as total_trips,
    count(distinct line) as total_lines,    
    current_timestamp() as built_at
from {{ref('stg_realtime_delays')}} 
