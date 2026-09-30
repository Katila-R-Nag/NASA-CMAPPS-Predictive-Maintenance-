-- Runs automatically the first time the warehouse container boots
-- (mounted into /docker-entrypoint-initdb.d by docker-compose.yml)

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw landing table for C-MAPSS sensor readings.
-- Deliberately unopinionated/loose: this is the "as-ingested" layer.
-- All cleaning/typing/renaming happens in dbt staging models, not here.
CREATE TABLE IF NOT EXISTS raw.sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    dataset_id      TEXT        NOT NULL,   -- e.g. 'FD001'
    unit_number     INTEGER     NOT NULL,
    time_in_cycles  INTEGER     NOT NULL,
    op_setting_1    DOUBLE PRECISION,
    op_setting_2    DOUBLE PRECISION,
    op_setting_3    DOUBLE PRECISION,
    sensor_1        DOUBLE PRECISION,
    sensor_2        DOUBLE PRECISION,
    sensor_3        DOUBLE PRECISION,
    sensor_4        DOUBLE PRECISION,
    sensor_5        DOUBLE PRECISION,
    sensor_6        DOUBLE PRECISION,
    sensor_7        DOUBLE PRECISION,
    sensor_8        DOUBLE PRECISION,
    sensor_9        DOUBLE PRECISION,
    sensor_10       DOUBLE PRECISION,
    sensor_11       DOUBLE PRECISION,
    sensor_12       DOUBLE PRECISION,
    sensor_13       DOUBLE PRECISION,
    sensor_14       DOUBLE PRECISION,
    sensor_15       DOUBLE PRECISION,
    sensor_16       DOUBLE PRECISION,
    sensor_17       DOUBLE PRECISION,
    sensor_18       DOUBLE PRECISION,
    sensor_19       DOUBLE PRECISION,
    sensor_20       DOUBLE PRECISION,
    sensor_21       DOUBLE PRECISION,
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_readings_unit
    ON raw.sensor_readings (dataset_id, unit_number, time_in_cycles);
