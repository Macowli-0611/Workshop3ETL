"""Copia el esquema, las vistas y los datos de la BD local a una Postgres en la nube
(p. ej. Neon) para que Looker Studio / Power BI / Tableau puedan consultarla.

La URL destino se lee de CLOUD_DATABASE_URL (en .env, que NO se versiona).
Es re-ejecutable: con --reset borra y recarga las tablas del destino.

    python scripts/sync_to_cloud.py --reset
"""
import argparse
import io
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2

from src import config

TABLES = ["dim_country", "dim_date", "raw_happiness_events", "fact_predictions"]  # orden por FKs
SEQUENCES = {"dim_country": "country_id", "raw_happiness_events": "raw_event_id", "fact_predictions": "prediction_id"}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reset", action="store_true", help="vaciar tablas del destino antes de copiar")
    args = ap.parse_args()

    url = os.getenv("CLOUD_DATABASE_URL")
    if not url:
        sys.exit("Define CLOUD_DATABASE_URL en .env (cadena de conexión de la BD en la nube)")

    src = psycopg2.connect(**config.PG)
    dst = psycopg2.connect(url)
    try:
        with dst, dst.cursor() as d:
            for f in ("create_tables.sql", "kpis.sql"):
                d.execute((config.ROOT / "sql" / f).read_text())
            if args.reset:
                d.execute("TRUNCATE fact_predictions, raw_happiness_events, dim_date, dim_country RESTART IDENTITY CASCADE")
            for t in TABLES:
                buf = io.StringIO()
                with src.cursor() as s:
                    s.copy_expert(f"COPY {t} TO STDOUT WITH (FORMAT csv)", buf)
                buf.seek(0)
                d.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv)", buf)
                print(f"{t}: copiada")
            for t, col in SEQUENCES.items():
                d.execute(f"SELECT setval(pg_get_serial_sequence('{t}','{col}'), COALESCE(MAX({col}),1)) FROM {t}")
            d.execute("SELECT COUNT(*) FROM fact_predictions")
            print("fact_predictions en destino:", d.fetchone()[0])
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
