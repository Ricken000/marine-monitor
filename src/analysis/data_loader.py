"""
data_loader.py — Carga y preparación de datos del motor marino.

Responsabilidades de este módulo:
  1. Leer el CSV histórico generado por el simulador del motor.
  2. Convertir la columna 'timestamp' en un índice temporal (DatetimeIndex),
     requerido por pandas para operaciones de series de tiempo (rolling, resample, etc.).
  3. Calcular estadísticas rodantes (media y desviación estándar) por parámetro,
     usadas luego por el detector de anomalías.
  4. Generar un resumen ejecutivo del dataset (tiempos, duración, tasa de fallas).

Formato esperado del CSV:
  - Columna 'timestamp': fechas en formato ISO 8601 (ej. 2024-01-15 08:30:00)
  - Columnas de sensores: rpm, temperature_exhaust, temperature_cooling,
                          pressure_lube, pressure_fuel, vibration_rms
  - Columna 'fault_injected': 0 (operación normal) o 1 (falla simulada)

Dependencias: pandas, numpy, pathlib (todas en requirements.txt)
"""
import pandas as pd
import numpy as np
from pathlib import Path


def load_engine_data(filepath: str) -> pd.DataFrame:
    """
    Lee el CSV del motor y lo deja listo para análisis de series temporales.

    Por qué usamos DatetimeIndex:
      pandas requiere un índice de tipo datetime para habilitar operaciones
      temporales como .rolling("1h") o .resample("5min"). Sin esto, esas
      funciones no saben interpretar el tiempo real entre muestras.

    Args:
        filepath: Ruta al archivo CSV (absoluta o relativa al directorio de trabajo).

    Returns:
        DataFrame indexado por timestamp, ordenado de más antiguo a más reciente.

    Raises:
        FileNotFoundError: Si el archivo no existe en la ruta indicada.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {filepath}")

    # parse_dates convierte la columna 'timestamp' a dtype datetime64
    # en lugar de dejarlo como string, que es el comportamiento por defecto.
    df = pd.read_csv(filepath, parse_dates=["timestamp"])

    # Al usar 'timestamp' como índice, todas las operaciones temporales
    # (rolling, resample, slicing por fecha) quedan disponibles directamente.
    df.set_index("timestamp", inplace=True)

    # Garantizamos orden cronológico ascendente. El CSV puede llegar desordenado
    # si se concatenaron archivos de distintas sesiones del simulador.
    df.sort_index(inplace=True)

    # Advertencia no bloqueante: los NaN no se imputan aquí para no alterar
    # los datos originales; cada función downstream decide cómo tratarlos.
    if df.isnull().sum().sum() > 0:
        print("Advertencia: el dataset contiene valores nulos")

    return df


def compute_rolling_stats(
    df: pd.DataFrame,
    window: str = "1h"
) -> pd.DataFrame:
    """
    Calcula media y desviación estándar con ventana rodante para cada sensor.

    Qué es una ventana rodante y para qué sirve aquí:
      En cada instante t, la ventana rodante mira hacia atrás durante 'window'
      tiempo (ej. 1 hora) y calcula la estadística sobre esas muestras.
      Resultado: una señal suavizada que revela tendencias y oculta ruido puntual.
      El detector de anomalías usa la desviación estándar para detectar cuándo
      un sensor se comporta de forma inusual respecto a su propio historial reciente.

    Nota sobre los primeros registros:
      Las primeras muestras del DataFrame tendrán NaN en las columnas _mean/_std
      porque no hay suficiente historia previa para llenar la ventana. Esto es
      esperado y no indica un problema en los datos.

    Args:
        df:     DataFrame con DatetimeIndex (salida de load_engine_data).
        window: Tamaño de la ventana temporal. Ejemplos válidos: '30min', '1h', '2h'.
                Ver documentación de pandas offset aliases para más opciones.

    Returns:
        DataFrame con las mismas filas que df, y columnas '<sensor>_mean' y
        '<sensor>_std' para cada sensor presente en df.
        Las columnas de sensores que no existan en df se omiten silenciosamente.
    """
    # Sensores físicos del motor marino monitoreados por el sistema.
    # Si en el futuro se agregan nuevos sensores al simulador, añadirlos aquí.
    params = [
        "rpm", "temperature_exhaust", "temperature_cooling",
        "pressure_lube", "pressure_fuel", "vibration_rms"
    ]

    # DataFrame vacío con el mismo índice temporal: iremos añadiendo columnas.
    stats = pd.DataFrame(index=df.index)

    for col in params:
        # Tolerancia ante datasets parciales: si un sensor no está en este CSV
        # (ej. versión anterior del simulador sin ese sensor), se salta sin error.
        if col not in df.columns:
            continue

        rolling = df[col].rolling(window=window)

        # round(3) evita ruido de punto flotante en los decimales al exportar.
        stats[f"{col}_mean"] = rolling.mean().round(3)
        stats[f"{col}_std"]  = rolling.std().round(3)

    return stats


def get_summary(df: pd.DataFrame) -> dict:
    """
    Genera un resumen ejecutivo del dataset como diccionario.

    Útil para logging al inicio de la app, reportes automáticos y
    para verificar rápidamente que el CSV cargado es el correcto
    antes de correr el pipeline de detección.

    Args:
        df: DataFrame con DatetimeIndex y columna 'fault_injected'
            (salida de load_engine_data).

    Returns:
        Diccionario con las siguientes claves:
          - total_readings:  número total de muestras en el dataset.
          - start_time:      timestamp de la primera muestra (string ISO).
          - end_time:        timestamp de la última muestra (string ISO).
          - duration_hours:  duración total del periodo en horas (float, 1 decimal).
          - fault_rate:      fracción de muestras con falla inyectada (0.0 – 1.0).
          - fault_count:     número absoluto de muestras con falla inyectada.
    """
    return {
        "total_readings":  len(df),
        "start_time":      str(df.index.min()),
        "end_time":        str(df.index.max()),
        # total_seconds() / 3600 convierte el timedelta a horas decimales.
        "duration_hours":  round(
            (df.index.max() - df.index.min()).total_seconds() / 3600, 1
        ),
        # .mean() sobre una columna binaria (0/1) da directamente la proporción.
        "fault_rate":      round(df["fault_injected"].mean(), 4),
        "fault_count":     int(df["fault_injected"].sum()),
    }
