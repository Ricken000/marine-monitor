"""
inspect_dataset.py — Inspección exploratoria del dataset de telemetría.

¿Qué hace este script?
    Carga el CSV generado por engine_simulator.py y realiza cuatro verificaciones
    en orden, mostrando los resultados en consola:

    [1] Estructura básica:
        Muestra cuántas lecturas tiene el dataset, qué período temporal cubre
        y qué columnas contiene. Sirve para confirmar que el archivo se generó
        correctamente y tiene el tamaño esperado.

    [2] Tipos de datos:
        Verifica que cada columna tiene el tipo correcto (float64 para sensores,
        bool para fault_injected). Un tipo inesperado como 'object' indicaría
        un problema al leer el CSV.

    [3] Valores nulos:
        El simulador no produce nulos por diseño. Si aparecen aquí, indica
        corrupción en el archivo (líneas incompletas, errores de escritura).

    [4] Estadísticas descriptivas:
        Muestra media, mínimo, máximo y percentiles de cada sensor.
        Útil para confirmar que los rangos de simulación son realistas.

    [5] Verificación de rangos operacionales:
        Compara los valores de las lecturas "normales" (sin falla inyectada)
        contra los límites del motor MAN B&W. Todos deben marcar "OK".
        Un "NOT OK" indica un bug en el simulador.

    [6] Análisis de fallas:
        Muestra cuántas fallas se inyectaron y sus estadísticas, para confirmar
        que los valores de falla son visiblemente distintos de los normales.
        Ej.: temperature_exhaust durante falla debe estar ≥ 410°C vs 320–380°C normal.

Cómo ejecutarlo (desde marine-monitor/):
    python notebooks/inspect_dataset.py

Requisito previo:
    El CSV data/raw/engine_readings_24h.csv debe existir. Generarlo con:
        python -m src.simulator.engine_simulator
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Agrega marine-monitor/ al path; necesario si se ejecuta desde otra carpeta
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Carga del dataset ──────────────────────────────────────────────────────
# Ruta absoluta basada en la ubicación del script, no del directorio de trabajo.
# parse_dates convierte 'timestamp' de string ISO 8601 a datetime64,
# lo que habilita indexado temporal, resampling y slicing por fecha.
DATA_PATH = Path(__file__).parent.parent / "data" / "raw" / "engine_readings_24h.csv"
df = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])

# Usar timestamp como índice facilita resampling y slicing temporal posterior
# (ej: df["2026-02-23 10:00":"2026-02-23 11:00"] para analizar una hora concreta)
df.set_index("timestamp", inplace=True)

print("=" * 55)
print("INSPECCIÓN DEL DATASET - MOTOR MARINO")
print("=" * 55)

# ── Estructura básica ──────────────────────────────────────────────────────
# df.shape → (n_filas, n_columnas); df.index.min/max dan el rango temporal cubierto
print(f"\n Dimensiones: {df.shape[0]} lecturas x {df.shape[1]} columnas")
print(f" Período: {df.index.min()} → {df.index.max()}")
print(f"\nColumnas: {list(df.columns)}")

# ── Tipos de datos ─────────────────────────────────────────────────────────
# Esperado: float64 para sensores, bool para fault_injected.
# Si alguna columna numérica aparece como object, hubo problema al leer el CSV.
print("\n-- Tipos de datos --")
print(df.dtypes)

# ── Valores nulos ──────────────────────────────────────────────────────────
# El simulador no genera nulos por diseño, pero esta validación detecta
# corrupciones en el CSV (líneas incompletas, errores de escritura, etc.)
print("\n── Valores nulos ──")
nulls = df.isnull().sum()
if nulls.sum() == 0:
    print("✅ Sin valores nulos")
else:
    # Mostrar solo las columnas que tienen al menos un nulo
    print(nulls[nulls > 0])

# ── Estadísticas descriptivas ──────────────────────────────────────────────
# select_dtypes(include=np.number) excluye 'fault_injected' (bool se interpreta
# como numérico en pandas, pero aquí la excluimos para centrarnos en sensores)
print("\n── Estadísticas descriptivas ──")
numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
# .describe() calcula: count, mean, std, min, 25%, 50%, 75%, max
print(df[numeric_cols].describe().round(2).to_string())

# ── Verificación de rangos operacionales ──────────────────────────────────
# Comprueba que las lecturas marcadas como normales (sin falla inyectada)
# respetan los límites definidos por la especificación del motor MAN B&W.
# Cualquier "NOT OK" indicaría un bug en el simulador.
print("\n── Verificación de rangos operacionales ──")

# Rangos normales — deben coincidir exactamente con MarineEngineSimulator.NORMAL_RANGES
NORMAL_RANGES = {
    "rpm":                 (85, 105),
    "temperature_exhaust": (320, 380),
    "temperature_cooling": (70, 85),
    "pressure_lube":       (3.5, 5.0),
    "pressure_fuel":       (8.0, 10.0),
    "vibration_rms":       (1.2, 3.5),
}

# Filtrar solo lecturas sin falla inyectada para la validación de rangos.
# Las lecturas con fault_injected=True son intencionalmente fuera de rango
# y contaminarían la validación si se incluyeran aquí.
normal_df = df[df["fault_injected"] == False]

for param, (low, high) in NORMAL_RANGES.items():
    # Contar lecturas normales que violan el rango esperado
    out_of_range = normal_df[
        (normal_df[param] < low) | (normal_df[param] > high)
    ]
    status = "OK " if len(out_of_range) == 0 else "NOT OK"
    print(f"{status}  {param}: min={normal_df[param].min():.2f} "
          f"max={normal_df[param].max():.2f} "
          f"(esperado {low}–{high})")

# ── Análisis de fallas inyectadas ──────────────────────────────────────────
# Verifica que la tasa de fallas del dataset coincide con fault_probability=0.03
# y muestra estadísticas de los valores durante las fallas para confirmar
# que los rangos anormales son visiblemente distintos de los normales.
print("\n── Análisis de fallas inyectadas ──")
total_faults = df["fault_injected"].sum()
print(f"Total fallas: {total_faults} ({total_faults/len(df):.1%} del dataset)")

faults_df = df[df["fault_injected"] == True]
if len(faults_df) > 0:
    # Comparar con la tabla de estadísticas normales de arriba:
    # los percentiles de temperature_exhaust, pressure_lube y vibration_rms
    # deben mostrar outliers claros en las filas de fallas.
    print("\nValores durante fallas (comparar con rangos normales):")
    print(faults_df[numeric_cols].describe().round(2).to_string())

print("\n" + "=" * 55)
print("Inspección completa.")
