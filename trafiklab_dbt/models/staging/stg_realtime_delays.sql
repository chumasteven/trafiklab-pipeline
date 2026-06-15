select 
    route_short_name as line,
    trip_id,
    direction_id,
    stop_sequence,
    stop_id,
    stop_name,
    delay as delay_seconds,
    round(delay / 60.0, 1)     as delay_minutes,
    arrival_time as arrival_unix,
    timestamp_seconds(arrival_time) as arrival_ts,
    arrival_time > 0 as has_valid_arrival_time
from {{ source('trafiklab_staging', 'realtime_delays') }}
