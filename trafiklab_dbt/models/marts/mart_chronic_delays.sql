with line_perf as (
    select line, pct_on_time, stop_events
    from {{ ref('mart_line_performance') }}
    where stop_events >= 100
),

network as (
    -- ONE row: the network-wide baseline
    select
        avg(pct_on_time)    as net_avg_on_time,
        stddev(pct_on_time) as net_std_on_time
    from line_perf
)

select
    l.line,
    l.pct_on_time,
    l.stop_events,
    n.net_avg_on_time,
    -- the flag: is this line > 2σ BELOW the network average on-time?
    l.pct_on_time < (n.net_avg_on_time - 2 * n.net_std_on_time) as is_chronic
from line_perf l
cross join network n
order by l.pct_on_time asc