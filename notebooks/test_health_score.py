"""
test_health_score.py — Validación manual del módulo EngineHealthScorer.

¿Qué verifica este script?
    Comprueba que el cálculo del health score produce resultados coherentes
    con la realidad operacional del motor en tres escenarios distintos:

    Test 1 — Lectura individual normal vs crítica:
        Genera dos lecturas únicas a mano (una buena y una mala) y muestra
        el score de cada una. Sirve para verificar que el modelo sí discrimina
        entre operación normal y condiciones peligrosas.
        Resultado esperado: score normal ≈ 95–100, score crítico < 40.

    Test 2 — Dataset completo:
        Aplica el scorer sobre las 1440 lecturas del CSV histórico y muestra
        las primeras 3 filas con sus columnas health_score y health_status.
        Sirve para confirmar que add_health_score() funciona en producción.

    Test 3 — Distribución de estados:
        Muestra cuántas lecturas cayeron en cada nivel (OPTIMAL / GOOD / ...)
        con una barra visual. Con fault_prob=0.03 se espera que la mayoría
        estén en OPTIMAL/GOOD y solo una pequeña fracción en ALERT/CRITICAL.

Cómo ejecutarlo (desde marine-monitor/):
    python notebooks/test_health_score.py

Requisito previo:
    El CSV data/raw/engine_readings_24h.csv debe existir. Generarlo con:
        python -m src.simulator.engine_simulator
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.data_loader import load_engine_data
from src.analysis.health_score import EngineHealthScorer
import pandas as pd

# Cargar datos
DATA_PATH = Path(__file__).parent.parent / "data" / "raw" / "engine_readings_24h.csv"
df = load_engine_data(str(DATA_PATH))

scorer = EngineHealthScorer()

# ── Test 1: lectura individual ──────────────────────────────
print("── Test 1: lecturas individuales ──")

normal_row = pd.Series({
    "rpm": 95, "temperature_exhaust": 350,
    "temperature_cooling": 77, "pressure_lube": 4.2,
    "pressure_fuel": 9.0, "vibration_rms": 2.0
})

fault_row = pd.Series({
    "rpm": 95, "temperature_exhaust": 447,  # sobrecalentamiento
    "temperature_cooling": 77, "pressure_lube": 1.6,  # presión crítica
    "pressure_fuel": 9.0, "vibration_rms": 11.5  # vibración extrema
})

print(f"Score lectura normal  : {scorer.compute(normal_row)}")
print(f"Score lectura crítica : {scorer.compute(fault_row)}")

# ── Test 2: dataset completo ────────────────────────────────
print("\n── Test 2: dataset completo ──")
df_scored = scorer.add_health_score(df)

print(f"Columnas nuevas: {['health_score', 'health_status']}")
print(f"\nPrimeras 3 filas:")
print(df_scored[["health_score", "health_status"]].head(3).to_string())

# ── Test 3: resumen de estados ──────────────────────────────
print("\n── Test 3: distribución de estados ──")
summary = scorer.get_status_summary(df_scored)

for status in ["OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"]:
    data = summary[status]
    bar  = "█" * int(data["percent"] / 2)
    print(f"  {status:<10} {data['count']:>5} lecturas  "
          f"({data['percent']:>5.1f}%)  {bar}")

print(f"\n  Score promedio : {summary['avg_score']}")
print(f"  Score mínimo   : {summary['min_score']}")