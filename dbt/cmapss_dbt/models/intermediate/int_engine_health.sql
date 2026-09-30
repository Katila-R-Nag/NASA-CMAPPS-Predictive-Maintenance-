-- Intermediate: one composite "health indicator" per engine/cycle.
--
-- v2 (recalibrated). The first version measured drift as a % of each
-- sensor's raw value. C-MAPSS sensors are large numbers with tiny relative
-- change (all six moved only ~0.2-1.8% between early life and the last 10
-- cycles), so the score barely moved and no engine ever left "healthy".
--
-- Fix: measure drift in units of each sensor's own variability (a z-score),
-- so a sensor that is quiet but moves a lot counts as much as a noisy one.
--   1. baseline  = average of the engine's first N cycles (less noisy than
--                  a single cycle-1 reading)
--   2. scale     = fleet-wide standard deviation of each sensor
--   3. direction = sensors 2,3,4,11,15 rise as the engine degrades, sensor 7
--                  falls, so drift is signed to always mean "worse" = positive
--
-- Validated: between early life and the last 10 cycles, all six sensors
-- shift by roughly 2.4-2.8 fleet standard deviations, so the sensor list is
-- justified by the data, not just by the literature.
--
-- Scope: fleet-wide scaling assumes ONE operating condition (FD001, FD003).
-- FD002/FD004 have six operating conditions and would need scaling per
-- condition first.

with sensors as (

    select * from {{ ref('stg_cmapss__sensors') }}

),

fleet_scale as (

    select
        dataset_id,
        nullif(stddev_pop(sensor_2),  0) as sd_2,
        nullif(stddev_pop(sensor_3),  0) as sd_3,
        nullif(stddev_pop(sensor_4),  0) as sd_4,
        nullif(stddev_pop(sensor_7),  0) as sd_7,
        nullif(stddev_pop(sensor_11), 0) as sd_11,
        nullif(stddev_pop(sensor_15), 0) as sd_15
    from sensors
    group by dataset_id

),

baseline as (

    select
        dataset_id,
        unit_number,
        avg(sensor_2)  as b_2,
        avg(sensor_3)  as b_3,
        avg(sensor_4)  as b_4,
        avg(sensor_7)  as b_7,
        avg(sensor_11) as b_11,
        avg(sensor_15) as b_15
    from sensors
    where time_in_cycles <= {{ var('baseline_cycles', 10) }}
    group by dataset_id, unit_number

),

degradation as (

    select
        s.dataset_id,
        s.unit_number,
        s.time_in_cycles,
        (
              (s.sensor_2  - b.b_2)  / f.sd_2
            + (s.sensor_3  - b.b_3)  / f.sd_3
            + (s.sensor_4  - b.b_4)  / f.sd_4
            - (s.sensor_7  - b.b_7)  / f.sd_7
            + (s.sensor_11 - b.b_11) / f.sd_11
            + (s.sensor_15 - b.b_15) / f.sd_15
        ) / 6.0 as degradation_z
    from sensors s
    join baseline b
        on s.dataset_id = b.dataset_id and s.unit_number = b.unit_number
    join fleet_scale f
        on s.dataset_id = f.dataset_id

),

smoothed as (

    select
        dataset_id,
        unit_number,
        time_in_cycles,
        -- 5-cycle rolling average to reduce sensor noise before thresholding
        avg(degradation_z) over (
            partition by dataset_id, unit_number
            order by time_in_cycles
            rows between 4 preceding and current row
        ) as rolling_degradation_z
    from degradation

)

select
    dataset_id,
    unit_number,
    time_in_cycles,
    rolling_degradation_z,
    -- 1.0 = at baseline, falls toward 0 as drift grows. Negative drift
    -- (noise below baseline) is clamped to 0 so the score never exceeds 1.
    1.0 / (1.0 + greatest(rolling_degradation_z, 0)) as health_indicator
from smoothed
