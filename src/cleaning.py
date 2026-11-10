"""Armonización de los CSV de World Happiness (2015-2019) a un esquema unificado."""
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
YEARS = [2015, 2016, 2017, 2018, 2019]

# Nombre original de cada columna -> nombre unificado, por año
COLUMN_MAP = {
    2015: {"Country": "country", "Happiness Score": "happiness_score",
           "Economy (GDP per Capita)": "gdp", "Family": "family",
           "Health (Life Expectancy)": "health", "Freedom": "freedom",
           "Generosity": "generosity", "Trust (Government Corruption)": "corruption"},
    2016: {"Country": "country", "Happiness Score": "happiness_score",
           "Economy (GDP per Capita)": "gdp", "Family": "family",
           "Health (Life Expectancy)": "health", "Freedom": "freedom",
           "Generosity": "generosity", "Trust (Government Corruption)": "corruption"},
    2017: {"Country": "country", "Happiness.Score": "happiness_score",
           "Economy..GDP.per.Capita.": "gdp", "Family": "family",
           "Health..Life.Expectancy.": "health", "Freedom": "freedom",
           "Generosity": "generosity", "Trust..Government.Corruption.": "corruption"},
    2018: {"Country or region": "country", "Score": "happiness_score",
           "GDP per capita": "gdp", "Social support": "family",
           "Healthy life expectancy": "health", "Freedom to make life choices": "freedom",
           "Generosity": "generosity", "Perceptions of corruption": "corruption"},
}
COLUMN_MAP[2019] = COLUMN_MAP[2018]

# Mismo país escrito distinto según el año -> nombre canónico
COUNTRY_ALIASES = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "North Cyprus": "Northern Cyprus",
    "Macedonia": "North Macedonia",
    "Somaliland region": "Somaliland Region",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "Northern Cyprus": "Northern Cyprus",
}

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
TARGET = "happiness_score"
FINAL_COLUMNS = ["country", "year"] + FEATURES + [TARGET, "corruption_imputed"]


def load_year(year: int, raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Lee un CSV, convierte a numérico ('N/A' -> NaN) y renombra al esquema unificado."""
    df = pd.read_csv(raw_dir / f"{year}.csv", na_values=["N/A", "NA", ""])
    df = df.rename(columns=COLUMN_MAP[year])[list(COLUMN_MAP[year].values())].copy()
    df["country"] = df["country"].astype("string").str.strip().replace(COUNTRY_ALIASES)
    for col in FEATURES + [TARGET]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df["year"] = year
    return df


def clean(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Une los 5 años, imputa nulos con la mediana del año y devuelve el dataset unificado."""
    df = pd.concat([load_year(y, raw_dir) for y in YEARS], ignore_index=True)
    df["corruption_imputed"] = df["corruption"].isna()
    df["corruption"] = df.groupby("year")["corruption"].transform(lambda s: s.fillna(s.median()))
    df = df.dropna(subset=FEATURES + [TARGET])
    df = df.drop_duplicates(subset=["country", "year"])
    df["corruption_imputed"] = df["corruption_imputed"].astype(bool)
    df[FEATURES + [TARGET]] = df[FEATURES + [TARGET]].round(5)
    return df[FINAL_COLUMNS].sort_values(["year", "country"]).reset_index(drop=True)


if __name__ == "__main__":
    out = clean()
    path = RAW_DIR.parent / "processed" / "happiness_clean.csv"
    out.to_csv(path, index=False)
    print(out.shape, "->", path)
    print(out.isna().sum().sum(), "nulos;", out["corruption_imputed"].sum(), "imputados")
