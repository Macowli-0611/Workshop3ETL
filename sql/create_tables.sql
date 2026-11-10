-- Esquema de Workshop 3: raw (staging) + modelo dimensional de predicciones.
-- Se ejecuta automáticamente al crear el contenedor de Postgres (docker-entrypoint-initdb.d).

-- 1. RAW / STAGING: el mensaje de Kafka tal como llegó (no es una dimensión)
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id       BIGSERIAL PRIMARY KEY,
    event_id           TEXT,                      -- extraído del JSON si existe (puede ser NULL en inválidos)
    kafka_topic        TEXT        NOT NULL,
    kafka_partition    INTEGER     NOT NULL,
    kafka_offset       BIGINT      NOT NULL,
    kafka_key          TEXT,
    raw_payload        TEXT        NOT NULL,      -- mensaje original, sin transformar
    received_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status  TEXT        NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN ('RECEIVED','VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR')),
    error_detail       TEXT,
    -- idempotencia: el mismo mensaje de Kafka nunca se inserta dos veces
    CONSTRAINT uq_raw_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_raw_status   ON raw_happiness_events (processing_status);
CREATE INDEX IF NOT EXISTS ix_raw_event_id ON raw_happiness_events (event_id);

-- 2. DIMENSIONES
CREATE TABLE IF NOT EXISTS dim_country (
    country_id    SERIAL PRIMARY KEY,
    country_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id    INTEGER PRIMARY KEY,               -- yyyymmdd
    full_date  DATE    NOT NULL UNIQUE,
    year       SMALLINT NOT NULL,
    quarter    SMALLINT NOT NULL,
    month      SMALLINT NOT NULL,
    day        SMALLINT NOT NULL,
    day_name   TEXT     NOT NULL
);

-- 3. HECHOS
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id         BIGSERIAL PRIMARY KEY,
    raw_event_id          BIGINT  NOT NULL UNIQUE REFERENCES raw_happiness_events (raw_event_id),
    country_id            INTEGER NOT NULL REFERENCES dim_country (country_id),
    date_id               INTEGER NOT NULL REFERENCES dim_date (date_id),   -- fecha de event_time
    observation_year      SMALLINT NOT NULL,                                -- año histórico del dato
    actual_score          NUMERIC(8,5) NOT NULL,
    predicted_score       NUMERIC(8,5) NOT NULL,
    prediction_error      NUMERIC(8,5) NOT NULL,                            -- predicho - real
    abs_prediction_error  NUMERIC(8,5) NOT NULL,
    event_time            TIMESTAMP   NOT NULL,                             -- emitido por el producer
    prediction_timestamp  TIMESTAMPTZ NOT NULL DEFAULT now(),
    model_name            TEXT
);
CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions (country_id);
CREATE INDEX IF NOT EXISTS ix_fact_date    ON fact_predictions (date_id);
CREATE INDEX IF NOT EXISTS ix_fact_year    ON fact_predictions (observation_year);
