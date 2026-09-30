-- Staging: light cleaning only. No business logic here — that belongs in
-- intermediate/marts. This model exists so downstream models never touch
-- the raw schema directly.

with source as (

    select * from {{ source('raw', 'sensor_readings') }}

),

renamed as (

    select
        dataset_id,
        unit_number,
        time_in_cycles,
        op_setting_1,
        op_setting_2,
        op_setting_3,
        sensor_1,
        sensor_2,
        sensor_3,
        sensor_4,
        sensor_5,
        sensor_6,
        sensor_7,
        sensor_8,
        sensor_9,
        sensor_10,
        sensor_11,
        sensor_12,
        sensor_13,
        sensor_14,
        sensor_15,
        sensor_16,
        sensor_17,
        sensor_18,
        sensor_19,
        sensor_20,
        sensor_21,
        loaded_at
    from source
    where unit_number is not null
      and time_in_cycles is not null

)

select * from renamed
