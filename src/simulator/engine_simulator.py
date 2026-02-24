"""
engine_simulator.py
-------------------
Simulador de telemetría de motor marino de 4 tiempos.

Genera lecturas sintéticas de sensores con distribución uniforme dentro
de rangos operacionales reales basados en motores MAN B&W (serie L/K).
Incluye mecanismo de inyección de fallas para validar modelos de detección.

Uso principal:
    python src/simulator/engine_simulator.py
    → genera data/raw/engine_readings_24h.csv

Uso como módulo:
    from src.simulator.engine_simulator import MarineEngineSimulator
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional


class MarineEngineSimulator:
    """
    Simulador de motor marino de 4 tiempos.

    Emula el comportamiento de sensores de un motor MAN B&W usando
    distribuciones uniformes dentro de rangos operacionales definidos
    por manuales de mantenimiento de la industria naviera.

    Parámetros simulados:
        rpm                 Revoluciones por minuto del motor principal
        temperature_exhaust Temperatura de gases de escape (°C)
        temperature_cooling Temperatura del circuito de refrigeración (°C)
        pressure_lube       Presión del sistema de lubricación (bar)
        pressure_fuel       Presión del sistema de combustible (bar)
        vibration_rms       Vibración RMS del cuerpo del motor (mm/s)
    """

    # Límites operacionales normales para cada sensor.
    # Cada valor es una tupla (mínimo, máximo) según especificación MAN B&W.
    # Lecturas fuera de estos rangos indican posible falla o desgaste.
    NORMAL_RANGES = {
        "rpm":                  (85, 105),      # Rango de crucero en mar abierto
        "temperature_exhaust":  (320, 380),     # °C — por encima de 400 indica combustión incompleta
        "temperature_cooling":  (70, 85),       # °C — por encima de 90 riesgo de cavitación
        "pressure_lube":        (3.5, 5.0),     # bar — mínima de seguridad: 2.5 bar
        "pressure_fuel":        (8.0, 10.0),    # bar — por debajo de 6 el motor se detiene automáticamente
        "vibration_rms":        (1.2, 3.5),     # mm/s — por encima de 7 indica desbalance o cojinetes dañados
    }

    def __init__(self, seed: Optional[int] = 42):
        """
        Inicializa el simulador con un generador de números aleatorios.

        Args:
            seed: Semilla para reproducibilidad. Con la misma semilla
                  siempre se genera el mismo dataset. None = aleatorio.

        Nota:
            np.random.default_rng es la API moderna de NumPy (>= 1.17).
            Es más eficiente y segura que np.random.seed() + np.random.uniform().
        """
        self.rng = np.random.default_rng(seed)

    def generate_normal_reading(self) -> dict:
        """
        Genera una lectura de sensor dentro de parámetros normales.

        Para cada parámetro en NORMAL_RANGES, muestrea un valor con
        distribución uniforme entre (mínimo, máximo). Esto simula la
        variabilidad natural del motor en condiciones estables.

        Returns:
            dict con un valor por sensor + timestamp ISO 8601 (UTC)
                 + fault_injected = False.
        """
        reading = {}

        # Iterar sobre cada sensor y generar un valor aleatorio en rango normal
        for param, (low, high) in self.NORMAL_RANGES.items():
            # uniform(low, high) → distribución plana entre los límites
            # round(..., 2)      → dos decimales para simular precisión de sensor industrial
            reading[param] = round(self.rng.uniform(low, high), 2)

        # Timestamp en formato ISO 8601 (UTC) — compatible con pandas parse_dates
        reading["timestamp"] = datetime.now(timezone.utc).isoformat()

        # Marcar lectura como normal (sin falla inyectada artificialmente)
        reading["fault_injected"] = False

        return reading

    def inject_fault(self, reading: dict, fault_type: str) -> dict:
        """
        Inyecta una falla conocida sobrescribiendo uno o más sensores.

        Las fallas están calibradas para simular degradaciones reales:
        - overheating:        combustión incompleta / fouling en turbo
        - low_lube_pressure:  bomba de aceite desgastada o fuga interna
        - high_vibration:     desbalance de hélice o cojinetes dañados

        El flag fault_injected = True permite evaluar la precisión de
        modelos de detección comparando predicciones vs etiquetas reales.

        Args:
            reading:    Lectura normal generada por generate_normal_reading().
            fault_type: Tipo de falla. Opciones:
                        'overheating' | 'low_lube_pressure' | 'high_vibration'

        Returns:
            dict con los parámetros afectados fuera de rango normal
            y fault_injected = True.
        """
        # Diccionario de fallas: mapea tipo → {parámetro afectado: (rango_anormal)}
        faults = {
            "overheating":       {"temperature_exhaust": (410, 460)},  # +30–80°C sobre límite
            "low_lube_pressure": {"pressure_lube":       (1.5, 2.5)},  # por debajo del mínimo de seguridad
            "high_vibration":    {"vibration_rms":       (7.0, 12.0)}, # 2-4x el valor normal máximo
        }

        if fault_type in faults:
            # Sobrescribir solo los parámetros afectados; el resto permanece normal
            for param, (low, high) in faults[fault_type].items():
                reading[param] = round(self.rng.uniform(low, high), 2)
                reading["fault_injected"] = True

        return reading

    def generate_dataset(
            self,
            hours: int = 24,
            interval_seconds: int = 60,
            fault_probability: float = 0.03,
    ) -> pd.DataFrame:
        """
        Genera un dataset completo de telemetría con fallas aleatorias.

        Simula una guardia de máquinas real: lecturas cada `interval_seconds`
        durante `hours` horas. Cada lectura tiene una probabilidad
        `fault_probability` de contener una falla inyectada aleatoria.

        Args:
            hours:             Duración total a simular (default: 24h = 1 guardia completa).
            interval_seconds:  Frecuencia de muestreo en segundos (default: 60s = 1 lectura/min).
            fault_probability: Probabilidad por lectura de inyectar una falla (default: 3%).

        Returns:
            pd.DataFrame con columnas:
                rpm, temperature_exhaust, temperature_cooling,
                pressure_lube, pressure_fuel, vibration_rms,
                timestamp (str ISO 8601), fault_injected (bool)

        Ejemplo:
            24h × (3600s/h ÷ 60s) = 1440 lecturas totales
            1440 × 0.03 ≈ 43 fallas esperadas
        """
        readings = []

        # El dataset inicia `hours` horas atrás para que el CSV represente
        # datos "históricos" desde el punto de vista de ejecución del script
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Total de lecturas = duración en segundos / intervalo de muestreo
        n_readings = (hours * 3600) // interval_seconds

        for i in range(n_readings):
            # 1. Generar lectura base dentro de rangos normales
            reading = self.generate_normal_reading()

            # 2. Asignar timestamp secuencial (simulando reloj real del barco)
            reading["timestamp"] = (
                start_time + timedelta(seconds=i * interval_seconds)
            ).isoformat()

            # 3. Con probabilidad `fault_probability`, inyectar una falla aleatoria
            if self.rng.random() < fault_probability:
                # Elegir falla al azar con distribución uniforme entre los 3 tipos
                fault = self.rng.choice(
                    ["overheating", "low_lube_pressure", "high_vibration"]
                )
                reading = self.inject_fault(reading, fault)

            readings.append(reading)

        # Convertir lista de dicts → DataFrame (columnas automáticas por claves del dict)
        return pd.DataFrame(readings)


# ── Punto de entrada cuando se ejecuta el script directamente ──────────────
if __name__ == "__main__":
    from pathlib import Path

    # Parámetros de la simulación — modificar aquí para generar distintos datasets
    HOURS = 24
    FAULT_PROBABILITY = 0.03  # 3% de lecturas con falla inyectada

    print("=== Marine Engine Simulator ===")
    print(f"Generando {HOURS}h de datos ({HOURS * 60} lecturas)...")

    # Instanciar con semilla fija → dataset reproducible entre ejecuciones
    sim = MarineEngineSimulator(seed=42)
    df = sim.generate_dataset(
        hours=HOURS,
        interval_seconds=60,
        fault_probability=FAULT_PROBABILITY,
    )

    # Guardar en CSV — la carpeta data/raw/ debe existir antes de ejecutar
    output_path = Path("data/raw/engine_readings_24h.csv")
    df.to_csv(output_path, index=False)

    # Resumen de salida para verificar integridad del dataset generado
    print(f"Dataset guardado en {output_path}")
    print(f"Total lecturas:          {len(df)}")
    print(f"Fallas inyectadas:       {df['fault_injected'].sum()}")
    print(f"Tasa de fallas real:     {df['fault_injected'].mean():.1%}")
    print("\nPrimeras 3 lecturas:")
    print(df.head(3).to_string())
