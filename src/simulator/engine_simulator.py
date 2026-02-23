import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

class MarineEngineSimulator:
    """
    Simulador de motor marino de 4 tiempos.
    Parámetros basados en rangos operacionales reales de motores MAN B&W
    """

    NORMAL_RANGES = {
        "rpm":                  (85, 105),
        "temperature_exhaust":  (320, 380),     # ºC
        "temperature_cooling":  (70, 85),       # ºC
        "pressure_lube":        (3.5, 5.0),     # bar
        "pressure_fuel":        (8.0, 10.0),    # bar
        "vibration_rms":        (1.2, 3.5)      # mm/s

    }

    def __init__(self, seed: Optional[int] = 42):
        self.rng = np.random.default_rng(seed)
    
    def generate_normal_reading(self) -> dict:
        """Genera una lectura dentro de parámetros normales."""
        reading = {}
        for param, (low, high) in self.NORMAL_RANGES.items():
            reading[param] = round(self.rng.uniform(low, high), 2)
        reading ["timestamp"] = datetime.utcnow().isoformat()
        reading["fault_injected"] = False
        return reading
    
    def inject_fault(self, reading: dict, fault_type: str) -> dict:
        """
        Inyecta fallas conocidas para testing de detección.
        fault_type: 'overheating' | 'low_lube_pressure' | 'high_vibration'
        """
        faults = {
            "overheating":          {"temperature_exhaust": (410, 460),},
            "low_lube_pressure":    {"pressure_lube": (1.5, 2.5)},
            "high_vibration":       {"vibration_rms": (7.0, 12.0)},
        }
        if fault_type in faults:
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
        """Genera un dataset completo con fallas aleatorias"""
        readings = []
        start_time = datetime.utcnow() - timedelta(hours=hours)
        n_readings = (hours * 3600)  // interval_seconds

        for i in range (n_readings):
            reading = self.generate_normal_reading()
            reading ["timestamp"] = (
                start_time + timedelta(seconds=i * interval_seconds)
            ).isoformat()

            if self.rng.random() < fault_probability:
                fault = self.rng.choice(
                    ["overheating", "low_lube_pressure", "high_vibration"]
                )
                reading = self.inject_fault(reading, fault)

            readings.appends(reading)
        
        return pd.DataFrame(readings)
            

