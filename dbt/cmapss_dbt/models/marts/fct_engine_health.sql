-- Mart: the table the dashboard actually reads from. Fleet-level view of
-- every engine/cycle with an interpretable alert zone instead of a raw
-- RUL regression number — this is the "operations team" framing, not the
-- "ML leaderboard" framing.
--
-- Note on engine_max_cycle / rul_proxy: this only means "actual cycles
-- until failure" on training-style data (engines run to failure). If you
-- load true held-out test data (truncated before failure), don't trust
-- rul_proxy — it would just reflect the truncation point, not real RUL.

with health as (

    select * from {{ ref('int_engine_health') }}

),

engine_lifetime as (

    select
        dataset_id,
        unit_number,
        max(time_in_cycles) as engine_max_cycle
    from health
    group by 1, 2

),

final as (

    select
        h.dataset_id,
        h.unit_number,
        h.time_in_cycles,
        h.health_indicator,
        e.engine_max_cycle,
        e.engine_max_cycle - h.time_in_cycles as rul_proxy_cycles,
        case
            when h.health_indicator >= {{ var('healthy_threshold', 0.70) }} then 'healthy'
            when h.health_indicator >= {{ var('watch_threshold', 0.45) }} then 'watch'
            else 'critical'
        end as alert_zone

    from health h
    left join engine_lifetime e
        on h.dataset_id = e.dataset_id
       and h.unit_number = e.unit_number

)

select * from final
