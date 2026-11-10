"""Configuración leída de variables de entorno (.env)."""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "happiness-events")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "happiness-ml-team")

PG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5433")),
    dbname=os.getenv("POSTGRES_DB", "happiness"),
    user=os.getenv("POSTGRES_USER", "happiness"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)

PROCESSED_CSV = ROOT / "data" / "processed" / "happiness_clean.csv"
MODEL_PATH = ROOT / "models" / "model.pkl"
