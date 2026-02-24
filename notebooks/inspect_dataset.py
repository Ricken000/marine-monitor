"""
inspect_dataset.py
------------------
Inspección exploratoria del dataset de telemetría generado por engine_simulator.py.

Valida que:
  - El CSV cargó correctamente (dimensiones, tipos, nulos).
  - Las lecturas normales (fault_injected=False) caen dentro de los rangos operacionales.
  - Las fallas inyectadas producen valores fuera de rango detectables.

Ejecutar desde la raíz del proyecto:
    python notebooks/inspect_dataset.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── Carga del dataset ──────────────────────────────────────────────────────
# parse_dates convierte la columna 'timestamp' de string ISO 8601 a datetime64,
# lo que permite indexar por tiempo y hacer aritmética de fechas directamente.
df = pd.read_csv(
    "data/raw/engine_readings_24h.csv",
    parse_dates=["timestamp"],
)

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
