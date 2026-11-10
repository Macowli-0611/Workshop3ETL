"""Producer: lee el dataset limpio registro a registro y lo publica como eventos JSON.

Uso:
    python kafka/producer.py --delay 0.2
    python kafka/producer.py --delay 0 --inject-errors 0.1 --limit 100   # incluye eventos corruptos a propósito
    python kafka/producer.py --dry-run --limit 20                        # no usa Kafka: escribe data/streaming/sample_events.jsonl
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from confluent_kafka import Producer

from src import config
from src.contract import FEATURES

# Nombre en el dataset unificado -> nombre en el contrato del evento
FIELD_MAP = {c: c for c in FEATURES}

CORRUPTIONS = [
    ("missing_field", lambda e: e.pop("gdp", None)),
    ("string_instead_of_number", lambda e: e.update(health="alto")),
    ("negative_value", lambda e: e.update(freedom=-1.5)),
    ("out_of_range_score", lambda e: e.update(actual_happiness_score=99)),
    ("bad_timestamp", lambda e: e.update(event_time="no-es-fecha")),
    ("empty_country", lambda e: e.update(country="")),
    ("null_feature", lambda e: e.update(corruption=None)),
]


def build_event(row: pd.Series, n: int) -> dict:
    return {
        "event_id": f"E{n:05d}",
        "country": row["country"],
        "year": int(row["year"]),
        **{c: float(row[c]) for c in FEATURES},
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now().replace(microsecond=0).isoformat(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--delay", type=float, default=0.2, help="segundos entre eventos (default 0.2)")
    ap.add_argument("--limit", type=int, default=None, help="máximo de eventos a enviar")
    ap.add_argument("--shuffle", action="store_true", help="orden aleatorio en lugar de cronológico")
    ap.add_argument("--inject-errors", type=float, default=0.0, metavar="FRAC",
                    help="fracción (0-1) de eventos que se corrompen a propósito")
    ap.add_argument("--dry-run", action="store_true",
                    help="no envía a Kafka; guarda los eventos en data/streaming/sample_events.jsonl")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--topic", default=config.KAFKA_TOPIC)
    args = ap.parse_args()

    df = pd.read_csv(config.PROCESSED_CSV)
    if args.shuffle:
        df = df.sample(frac=1, random_state=args.seed)
    if args.limit:
        df = df.head(args.limit)

    rng = random.Random(args.seed)
    producer = None
    dump = None
    if args.dry_run:
        dump_path = config.ROOT / "data" / "streaming" / "sample_events.jsonl"
        dump = open(dump_path, "w", encoding="utf-8")
    else:
        producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "client.id": "happiness-producer",
                             "enable.idempotence": True, "acks": "all"})
    stats = {"sent": 0, "corrupted": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"ERROR entrega: {err}", file=sys.stderr)

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        event = build_event(row, i)
        tag = ""
        if args.inject_errors and rng.random() < args.inject_errors:
            name, fn = rng.choice(CORRUPTIONS)
            fn(event)
            stats["corrupted"] += 1
            tag = f"  [CORRUPTO: {name}]"
        body = json.dumps(event, ensure_ascii=False)
        if dump:
            dump.write(body + "\n")
        else:
            producer.produce(args.topic, key=str(row["country"]).encode("utf-8"),
                             value=body.encode("utf-8"), on_delivery=on_delivery)
            producer.poll(0)
        stats["sent"] += 1
        print(f"→ {event['event_id']} {row['country']} {int(row['year'])}{tag}")
        if args.delay > 0:
            time.sleep(args.delay)

    if producer:
        producer.flush(30)
    if dump:
        dump.close()
        print(f"\n(dry-run) eventos guardados en {dump_path}")
    print(f"\nListo: {stats['sent']} eventos enviados, {stats['corrupted']} corruptos, {stats['failed']} fallidos")


if __name__ == "__main__":
    main()
