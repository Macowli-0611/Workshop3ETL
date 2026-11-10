"""Contrato del evento `happiness-events` y su validación.

Kafka solo transporta bytes; la aplicación debe interpretar y validar los campos de negocio.
Estados: VALID | INVALID_SCHEMA (estructura/campos/tipos) | INVALID_VALUES (valores fuera de rango o mal formados).
"""
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]  # orden de entrenamiento
REQUIRED_FIELDS = ["event_id", "country", "year"] + FEATURES + ["actual_happiness_score", "event_time"]

FEATURE_RANGE = (0.0, 3.0)   # los indicadores publicados van de 0 a ~2.1
SCORE_RANGE = (0.0, 10.0)    # escala Cantril
YEAR_RANGE = (1990, 2100)

VALID, INVALID_SCHEMA, INVALID_VALUES, PREDICTION_ERROR = "VALID", "INVALID_SCHEMA", "INVALID_VALUES", "PREDICTION_ERROR"


@dataclass
class ValidationResult:
    status: str
    detail: Optional[str] = None
    event: Optional[dict] = None   # evento ya tipado (solo si VALID)

    @property
    def ok(self) -> bool:
        return self.status == VALID


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_event(payload: Any) -> ValidationResult:
    """Valida un mensaje (bytes/str/dict) contra el contrato. Nunca lanza excepciones."""
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        if isinstance(payload, str):
            payload = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return ValidationResult(INVALID_SCHEMA, f"JSON ilegible: {exc}")

    if not isinstance(payload, dict):
        return ValidationResult(INVALID_SCHEMA, "el mensaje no es un objeto JSON")

    # 1) Esquema: campos requeridos y tipos
    missing = [f for f in REQUIRED_FIELDS if f not in payload or payload[f] is None]
    if missing:
        return ValidationResult(INVALID_SCHEMA, f"campos faltantes: {missing}")

    for f in ("event_id", "country", "event_time"):
        if not isinstance(payload[f], str):
            return ValidationResult(INVALID_SCHEMA, f"'{f}' debe ser string")
    if not isinstance(payload["year"], int) or isinstance(payload["year"], bool):
        return ValidationResult(INVALID_SCHEMA, "'year' debe ser entero")
    bad_type = [f for f in FEATURES + ["actual_happiness_score"] if not _is_number(payload[f])]
    if bad_type:
        return ValidationResult(INVALID_SCHEMA, f"campos no numéricos: {bad_type}")

    # 2) Valores
    if not payload["event_id"].strip():
        return ValidationResult(INVALID_VALUES, "'event_id' vacío")
    if not payload["country"].strip():
        return ValidationResult(INVALID_VALUES, "'country' vacío")
    if not YEAR_RANGE[0] <= payload["year"] <= YEAR_RANGE[1]:
        return ValidationResult(INVALID_VALUES, f"'year' fuera de rango {YEAR_RANGE}: {payload['year']}")
    for f in FEATURES:
        v = float(payload[f])
        if not math.isfinite(v) or not FEATURE_RANGE[0] <= v <= FEATURE_RANGE[1]:
            return ValidationResult(INVALID_VALUES, f"'{f}' fuera de rango {FEATURE_RANGE}: {payload[f]}")
    s = float(payload["actual_happiness_score"])
    if not math.isfinite(s) or not SCORE_RANGE[0] <= s <= SCORE_RANGE[1]:
        return ValidationResult(INVALID_VALUES, f"'actual_happiness_score' fuera de rango {SCORE_RANGE}: {s}")
    try:
        event_time = datetime.fromisoformat(payload["event_time"])
    except ValueError:
        return ValidationResult(INVALID_VALUES, f"'event_time' no es ISO-8601: {payload['event_time']!r}")

    clean = {
        "event_id": payload["event_id"].strip(),
        "country": payload["country"].strip(),
        "year": payload["year"],
        **{f: float(payload[f]) for f in FEATURES},
        "actual_happiness_score": s,
        "event_time": event_time,
    }
    return ValidationResult(VALID, None, clean)
