"""Pruebas de integración del consumer contra Postgres (se omiten si la BD no está levantada).

Usan un tópico falso ('pytest-topic') y limpian lo que insertan.
"""
import json
import sys
from pathlib import Path

import joblib
import psycopg2
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "kafka"))
from src import config  # noqa: E402

try:
    CONN = psycopg2.connect(**config.PG, connect_timeout=3)
    CONN.autocommit = True
except Exception:  # BD no disponible
    CONN = None

pytestmark = pytest.mark.skipif(CONN is None, reason="Postgres no disponible (docker compose up -d)")

import consumer  # noqa: E402

TOPIC = "pytest-topic"
GOOD = {"event_id": "T001", "country": "Testland", "year": 2019, "gdp": 1.2, "family": 0.8, "health": 0.9,
        "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "actual_happiness_score": 6.2,
        "event_time": "2026-10-30T15:30:00"}


class FakeMsg:
    def __init__(self, value, offset, partition=0):
        self._v = value if isinstance(value, bytes) else json.dumps(value).encode()
        self._o, self._p = offset, partition

    def topic(self): return TOPIC
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return b"Testland"
    def value(self): return self._v


class BrokenModel:
    def predict(self, X): raise RuntimeError("modelo roto")


class NanModel:
    def predict(self, X): return [float("nan")]


@pytest.fixture(autouse=True)
def cleanup():
    yield
    with CONN.cursor() as c:
        c.execute("DELETE FROM fact_predictions WHERE raw_event_id IN (SELECT raw_event_id FROM raw_happiness_events WHERE kafka_topic=%s)", (TOPIC,))
        c.execute("DELETE FROM raw_happiness_events WHERE kafka_topic=%s", (TOPIC,))
        c.execute("DELETE FROM dim_country WHERE country_name='Testland' AND country_id NOT IN (SELECT country_id FROM fact_predictions)")


@pytest.fixture(scope="module")
def model():
    return joblib.load(config.MODEL_PATH)


def row(offset):
    with CONN.cursor() as c:
        c.execute("SELECT processing_status, error_detail, raw_payload FROM raw_happiness_events WHERE kafka_topic=%s AND kafka_offset=%s", (TOPIC, offset))
        return c.fetchone()


def facts():
    with CONN.cursor() as c:
        c.execute("SELECT COUNT(*) FROM fact_predictions f JOIN raw_happiness_events r USING (raw_event_id) WHERE r.kafka_topic=%s", (TOPIC,))
        return c.fetchone()[0]


def test_valid_event_creates_fact_linked_to_raw(model):
    assert consumer.process(CONN.cursor(), model, "LinearRegression", FakeMsg(GOOD, 1)) == "VALID"
    assert row(1)[0] == "VALID" and facts() == 1
    with CONN.cursor() as c:
        c.execute("""SELECT f.actual_score, f.predicted_score, f.prediction_error FROM fact_predictions f
                     JOIN raw_happiness_events r USING (raw_event_id) WHERE r.kafka_topic=%s""", (TOPIC,))
        actual, pred, err = c.fetchone()
    assert float(err) == pytest.approx(float(pred) - float(actual), abs=1e-4)


def test_raw_payload_is_stored_exactly_as_received(model):
    payload = b'{"event_id":"T002",  "country":"Testland"}'
    consumer.process(CONN.cursor(), model, "x", FakeMsg(payload, 2))
    assert row(2)[2] == payload.decode()


@pytest.mark.parametrize("payload,status", [
    (b"no es json", "INVALID_SCHEMA"),
    ({**GOOD, "gdp": "alto"}, "INVALID_SCHEMA"),
    ({**GOOD, "freedom": -3}, "INVALID_VALUES"),
])
def test_invalid_events_are_stored_and_skipped(model, payload, status):
    assert consumer.process(CONN.cursor(), model, "x", FakeMsg(payload, 3)) == status
    assert row(3)[0] == status and facts() == 0


def test_prediction_error_does_not_crash_and_is_marked():
    assert consumer.process(CONN.cursor(), BrokenModel(), "x", FakeMsg(GOOD, 4)) == "PREDICTION_ERROR"
    status, detail, _ = row(4)
    assert status == "PREDICTION_ERROR" and "modelo roto" in detail and facts() == 0


def test_non_finite_prediction_is_prediction_error():
    assert consumer.process(CONN.cursor(), NanModel(), "x", FakeMsg(GOOD, 5)) == "PREDICTION_ERROR"
    assert facts() == 0


def test_replaying_same_kafka_message_is_idempotent(model):
    msg = FakeMsg(GOOD, 6)
    assert consumer.process(CONN.cursor(), model, "x", msg) == "VALID"
    assert consumer.process(CONN.cursor(), model, "x", msg) == "VALID"   # reproceso: ya estaba VALID
    assert facts() == 1
    with CONN.cursor() as c:
        c.execute("SELECT COUNT(*) FROM raw_happiness_events WHERE kafka_topic=%s", (TOPIC,))
        assert c.fetchone()[0] == 1
