import copy
from datetime import datetime

import pytest

from src.contract import (INVALID_SCHEMA, INVALID_VALUES, VALID, FEATURES, validate_event)

GOOD = {"event_id": "E001", "country": "Colombia", "year": 2019, "gdp": 1.2, "family": 0.8, "health": 0.9,
        "freedom": 0.6, "generosity": 0.3, "corruption": 0.1, "actual_happiness_score": 6.2,
        "event_time": "2026-10-30T15:30:00"}


def mut(**kw):
    d = copy.deepcopy(GOOD)
    d.update(kw)
    return d


def test_valid_event_is_typed():
    r = validate_event(GOOD)
    assert r.ok and r.event["event_time"] == datetime(2026, 10, 30, 15, 30)
    assert r.event["gdp"] == 1.2


def test_accepts_json_string_and_bytes():
    import json
    assert validate_event(json.dumps(GOOD)).ok
    assert validate_event(json.dumps(GOOD).encode()).ok


@pytest.mark.parametrize("payload", [b"\xff\xfe", "no es json", "[1,2]", "42", None])
def test_unreadable_or_not_object(payload):
    assert validate_event(payload).status == INVALID_SCHEMA


@pytest.mark.parametrize("field", ["event_id", "country", "year", "event_time", "actual_happiness_score"] + FEATURES)
def test_missing_field(field):
    d = {k: v for k, v in GOOD.items() if k != field}
    assert validate_event(d).status == INVALID_SCHEMA


def test_null_field_counts_as_missing():
    assert validate_event(mut(gdp=None)).status == INVALID_SCHEMA


@pytest.mark.parametrize("kw", [{"gdp": "1.2"}, {"year": "2019"}, {"year": 2019.5}, {"gdp": True},
                                {"event_id": 5}, {"country": 7}])
def test_wrong_types(kw):
    assert validate_event(mut(**kw)).status == INVALID_SCHEMA


@pytest.mark.parametrize("kw", [{"gdp": -0.1}, {"freedom": 9.0}, {"corruption": float("nan")},
                                {"generosity": float("inf")}, {"actual_happiness_score": 11},
                                {"actual_happiness_score": -1}, {"year": 1800}, {"country": "  "},
                                {"event_id": ""}, {"event_time": "ayer"}, {"event_time": "2026-13-45T00:00:00"}])
def test_invalid_values(kw):
    assert validate_event(mut(**kw)).status == INVALID_VALUES


def test_extra_fields_are_ignored():
    assert validate_event(mut(extra="x")).status == VALID
