"""Consumer: inferencia en tiempo real sobre el tópico `happiness-events`.

Flujo por mensaje (orden exigido por el enunciado):
  1. recibir  2. guardar el mensaje CRUDO en raw_happiness_events  3. validar contrato
  4. ordenar features como en el entrenamiento  5. cargar modelo (una vez)  6. predecir
  7. guardar en fact_predictions

Garantías:
  - Un evento inválido se guarda en raw con su estado, no se predice y NO detiene el consumer.
  - El offset se confirma (commit manual) SOLO después de persistir el resultado -> sin pérdidas.
  - Idempotente: reprocesar un mensaje ya guardado no duplica filas (UNIQUE topic/partition/offset).

Uso:
    python kafka/consumer.py                    # corre hasta Ctrl+C
    python kafka/consumer.py --idle-timeout 15  # termina tras 15 s sin mensajes
"""
import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib
import pandas as pd
import psycopg2
from confluent_kafka import Consumer, KafkaError

from src import config
from src.contract import FEATURES, INVALID_SCHEMA, PREDICTION_ERROR, VALID, validate_event

log = logging.getLogger("consumer")
STOP = False


def _stop(*_):
    global STOP
    STOP = True


def connect_db():
    """Conecta a Postgres con reintentos."""
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(**config.PG)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("BD no disponible (intento %d/10): %s", attempt, str(exc).strip())
            time.sleep(min(2 * attempt, 15))
    raise SystemExit("No se pudo conectar a la base de datos")


def decode_text(raw: bytes | None) -> str:
    """Texto del mensaje para guardarlo tal cual llegó (NUL no es válido en TEXT de Postgres)."""
    return (raw or b"").decode("utf-8", errors="replace").replace("\x00", "\\x00")


def store_raw(cur, msg) -> tuple[int, str, bool]:
    """Inserta el mensaje crudo. Devuelve (raw_event_id, status, es_nuevo)."""
    payload = decode_text(msg.value())
    key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
    event_id = None
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict) and isinstance(obj.get("event_id"), str):
            event_id = obj["event_id"]
    except ValueError:
        pass
    cur.execute(
        """INSERT INTO raw_happiness_events
               (event_id, kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload)
           VALUES (%s,%s,%s,%s,%s,%s)
           ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
           RETURNING raw_event_id""",
        (event_id, msg.topic(), msg.partition(), msg.offset(), key, payload),
    )
    row = cur.fetchone()
    if row:
        return row[0], "RECEIVED", True
    cur.execute("""SELECT raw_event_id, processing_status FROM raw_happiness_events
                   WHERE kafka_topic=%s AND kafka_partition=%s AND kafka_offset=%s""",
                (msg.topic(), msg.partition(), msg.offset()))
    rid, status = cur.fetchone()
    return rid, status, False


def set_status(cur, raw_id: int, status: str, detail: str | None = None) -> None:
    cur.execute("UPDATE raw_happiness_events SET processing_status=%s, error_detail=%s WHERE raw_event_id=%s",
                (status, detail, raw_id))


def get_country_id(cur, name: str) -> int:
    cur.execute("""INSERT INTO dim_country (country_name) VALUES (%s)
                   ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name
                   RETURNING country_id""", (name,))
    return cur.fetchone()[0]


def get_date_id(cur, d) -> int:
    date_id = int(d.strftime("%Y%m%d"))
    cur.execute("""INSERT INTO dim_date (date_id, full_date, year, quarter, month, day, day_name)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (date_id) DO NOTHING""",
                (date_id, d, d.year, (d.month - 1) // 3 + 1, d.month, d.day, d.strftime("%A")))
    return date_id


def process(cur, model, model_name: str, msg) -> str:
    """Procesa un mensaje y devuelve el estado final."""
    raw_id, status, is_new = store_raw(cur, msg)                      # (2) crudo primero
    if not is_new and status != "RECEIVED":
        return status                                                  # ya procesado: idempotencia

    result = validate_event(msg.value())                               # (3) contrato
    if not result.ok:
        set_status(cur, raw_id, result.status, result.detail)
        return result.status

    ev = result.event
    try:
        x = pd.DataFrame([{f: ev[f] for f in FEATURES}])[FEATURES]      # (4) mismo orden que el entrenamiento
        pred = float(model.predict(x)[0])                               # (5,6) predicción
        if pred != pred or pred in (float("inf"), float("-inf")):
            raise ValueError(f"predicción no finita: {pred}")
        actual = ev["actual_happiness_score"]
        cur.execute("BEGIN")
        country_id = get_country_id(cur, ev["country"])
        date_id = get_date_id(cur, ev["event_time"].date())
        cur.execute(
            """INSERT INTO fact_predictions
                   (raw_event_id, country_id, date_id, observation_year, actual_score, predicted_score,
                    prediction_error, abs_prediction_error, event_time, prediction_timestamp, model_name)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (raw_event_id) DO NOTHING""",
            (raw_id, country_id, date_id, ev["year"], actual, round(pred, 5), round(pred - actual, 5),
             round(abs(pred - actual), 5), ev["event_time"], datetime.now(timezone.utc), model_name),
        )
        set_status(cur, raw_id, VALID)
        cur.execute("COMMIT")                                           # (7) resultado persistido
        return VALID
    except psycopg2.OperationalError:
        raise                                                          # caída de BD: se reintenta el mensaje
    except Exception as exc:                                           # error de predicción u otro dato inesperado
        cur.execute("ROLLBACK")
        log.exception("Error prediciendo %s", ev["event_id"])
        set_status(cur, raw_id, PREDICTION_ERROR, f"{type(exc).__name__}: {exc}")
        return PREDICTION_ERROR


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--idle-timeout", type=float, default=None, help="salir tras N segundos sin mensajes")
    ap.add_argument("--max-messages", type=int, default=None)
    ap.add_argument("--from-beginning", action="store_true",
                    help="grupo nuevo desde el inicio (usa auto.offset.reset=earliest; ya es el default)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    model = joblib.load(config.MODEL_PATH)                              # modelo + preprocesamiento
    model_name = type(model.named_steps["model"]).__name__ if hasattr(model, "named_steps") else type(model).__name__
    log.info("Modelo cargado: %s", model_name)

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,                                    # commit manual tras persistir
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    conn = connect_db()
    counts: dict[str, int] = {}
    total, last_msg = 0, time.monotonic()
    log.info("Escuchando %s con el grupo %s ...", config.KAFKA_TOPIC, config.KAFKA_GROUP)

    try:
        while not STOP:
            msg = consumer.poll(1.0)
            if msg is None:
                if args.idle_timeout and time.monotonic() - last_msg > args.idle_timeout:
                    log.info("Sin mensajes por %.0f s; fin.", args.idle_timeout)
                    break
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka: %s", msg.error())
                continue
            last_msg = time.monotonic()
            while True:                                                 # reintenta el mismo mensaje si cae la BD
                try:
                    status = process(conn.cursor(), model, model_name, msg)
                    break
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    log.warning("BD caída (%s); reconectando", str(exc).strip())
                    conn = connect_db()
            consumer.commit(message=msg, asynchronous=False)           # offset solo tras persistir
            counts[status] = counts.get(status, 0) + 1
            total += 1
            log.info("p%d@%d %-12s %s", msg.partition(), msg.offset(), status, (msg.key() or b"").decode(errors="replace"))
            if args.max_messages and total >= args.max_messages:
                break
    finally:
        consumer.close()
        conn.close()
        log.info("Resumen: %d mensajes %s", total, counts)


if __name__ == "__main__":
    main()
