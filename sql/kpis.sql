-- KPIs y vistas para el dashboard (el BI consulta estas vistas, no CSV).

-- Se recrean desde cero: Postgres no permite cambiar el tipo de una columna con CREATE OR REPLACE VIEW.
DROP VIEW IF EXISTS v_kpi_avg_error, v_kpi_predictions_by_country, v_kpi_predicted_vs_actual,
                    v_kpi_trend_by_year, v_kpi_trend_over_time, v_kpi_pipeline_quality, v_predictions;

-- Vista base desnormalizada
CREATE OR REPLACE VIEW v_predictions AS
SELECT f.prediction_id, f.raw_event_id, r.event_id, c.country_name, d.full_date AS event_date,
       f.observation_year, f.actual_score, f.predicted_score, f.prediction_error,
       f.abs_prediction_error, f.event_time, f.prediction_timestamp, f.model_name
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
JOIN dim_date d    ON d.date_id = f.date_id
JOIN raw_happiness_events r ON r.raw_event_id = f.raw_event_id;

-- KPI 1: error promedio de predicción
CREATE OR REPLACE VIEW v_kpi_avg_error AS
SELECT COUNT(*)::numeric AS predictions_total,
       ROUND(AVG(abs_prediction_error), 4)   AS mae,
       ROUND(SQRT(AVG(prediction_error ^ 2)), 4) AS rmse,
       ROUND(AVG(prediction_error), 4)       AS bias
FROM fact_predictions;

-- KPI 2: predicciones por país
CREATE OR REPLACE VIEW v_kpi_predictions_by_country AS
SELECT country_name, COUNT(*)::numeric AS predictions,
       ROUND(AVG(predicted_score), 3) AS avg_predicted,
       ROUND(AVG(actual_score), 3)    AS avg_actual,
       ROUND(AVG(abs_prediction_error), 4) AS mae
FROM v_predictions GROUP BY country_name;

-- KPI 3: predicho vs real (nivel evento; para dispersión)
CREATE OR REPLACE VIEW v_kpi_predicted_vs_actual AS
SELECT event_id, country_name, observation_year, actual_score, predicted_score, prediction_error
FROM v_predictions;

-- KPI 4: tendencia por año histórico y por momento de predicción
CREATE OR REPLACE VIEW v_kpi_trend_by_year AS
SELECT observation_year, COUNT(*)::numeric AS predictions,
       ROUND(AVG(actual_score), 4) AS avg_actual, ROUND(AVG(predicted_score), 4) AS avg_predicted,
       ROUND(AVG(abs_prediction_error), 4) AS mae
FROM fact_predictions GROUP BY observation_year;

CREATE OR REPLACE VIEW v_kpi_trend_over_time AS
SELECT date_trunc('minute', prediction_timestamp) AS prediction_minute, COUNT(*)::numeric AS predictions,
       ROUND(AVG(abs_prediction_error), 4) AS mae,
       ROUND(AVG(predicted_score), 4) AS avg_predicted, ROUND(AVG(actual_score), 4) AS avg_actual
FROM fact_predictions GROUP BY 1;

-- Calidad del pipeline: estados del raw
CREATE OR REPLACE VIEW v_kpi_pipeline_quality AS
SELECT processing_status, COUNT(*)::numeric AS events FROM raw_happiness_events GROUP BY processing_status;
