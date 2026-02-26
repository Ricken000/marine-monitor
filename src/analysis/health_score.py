"""
health_score.py — Índice de salud del motor marino.

¿Qué hace este módulo?
    Toma las lecturas crudas de 6 sensores del motor (temperatura, presión,
    vibración, RPM) y las convierte en un único número de 0 a 100 que
    resume el estado del motor en ese instante — similar a un "semáforo"
    de salud para el maquinista.

¿Por qué un score unificado?
    En un motor marino hay docenas de parámetros. Decidir si el motor está
    bien o mal revisando cada uno por separado es lento y propenso a errores.
    Un score unificado permite:
      - Comparar el estado entre diferentes momentos del tiempo.
      - Disparar alertas automáticas sin lógica ad-hoc por sensor.
      - Comunicar el estado a personal no técnico (capitán, armador).

Cómo se interpreta el score:
    90 – 100  OPTIMAL   Operación normal. Sin acción requerida.
    75 –  89  GOOD      Monitoreo rutinario. Revisar en próxima guardia.
    60 –  74  CAUTION   Atención. Aumentar frecuencia de inspección.
    40 –  59  ALERT     Evaluar mantenimiento preventivo antes de zarpar.
     0 –  39  CRITICAL  Reducir carga del motor o detener para inspección.

Cómo se calcula el score:
    Cada sensor tiene un rango "óptimo" y un peso (importancia relativa).
    Si el valor del sensor está dentro del rango óptimo → contribuye 100 puntos.
    Si se aleja del rango → su aporte cae linealmente hasta llegar a 0.
    El score final es el promedio ponderado de todos los sensores.
    Si algún sensor cae en zona crítica extrema, se aplica una penalización
    adicional para bajar el score de forma más agresiva.

Uso típico:
    scorer = EngineHealthScorer()
    df_con_score = scorer.add_health_score(df)   # agrega columnas al DataFrame
    resumen      = scorer.get_status_summary(df_con_score)

Dependencias: pandas, numpy
"""
import pandas as pd
import numpy as np


class EngineHealthScorer:
    """
    Calcula el índice de salud (0–100) del motor a partir de sus sensores.

    Cada sensor tiene dos propiedades configuradas en PARAMETERS:
        optimal — rango de valores donde el motor opera sin problemas.
                  Fuera de ese rango el sensor empieza a penalizar el score.
        weight  — cuánto importa ese sensor en el score final (entre 0 y 1).
                  Los pesos suman exactamente 1.0; sensores más críticos
                  para la seguridad tienen mayor peso.

    Jerarquía de importancia (mayor peso = más impacto en el score):
        temperature_exhaust  0.25  — indicador principal de combustión
        pressure_lube        0.25  — sin lubricación el motor se destruye en minutos
        vibration_rms        0.20  — detecta desbalance y cojinetes dañados
        temperature_cooling  0.15  — refrigeración previene deformaciones térmicas
        rpm                  0.10  — velocidad de giro nominal
        pressure_fuel        0.05  — alimentación de combustible al inyector

    Uso rápido:
        scorer = EngineHealthScorer()
        score  = scorer.compute(row)              # score para una sola lectura
        df     = scorer.add_health_score(df)      # score para todo el dataset
    """

    PARAMETERS = {
        "temperature_exhaust": {"optimal": (330, 370), "weight": 0.25},
        "pressure_lube":       {"optimal": (3.8, 4.8), "weight": 0.25},
        "vibration_rms":       {"optimal": (1.5, 3.0), "weight": 0.20},
        "temperature_cooling": {"optimal": (72, 82),   "weight": 0.15},
        "rpm":                 {"optimal": (90, 100),  "weight": 0.10},
        "pressure_fuel":       {"optimal": (8.5, 9.5), "weight": 0.05},
    }

    def compute_parameter_score(
        self,
        value: float,
        optimal_low: float,
        optimal_high: float
    ) -> float:
        """
        Convierte la lectura de un sensor en un puntaje de 0 a 100.

        Regla simple: si el valor está dentro del rango óptimo → 100 puntos.
        Si se sale del rango, el puntaje baja de forma proporcional a cuánto
        se alejó, hasta llegar a 0 en el límite de lo tolerable.

        Ejemplo con presión de lubricación (rango óptimo 3.8 – 4.8 bar):
            4.2 bar  → dentro del rango          → 100 puntos
            3.4 bar  → ligeramente fuera          →  50 puntos
            2.8 bar  → muy fuera (peligroso)      →   0 puntos

        El "margen" es la mitad del ancho del rango óptimo. En el ejemplo:
            margen = (4.8 - 3.8) / 2 = 0.5 bar
            Caer 0.5 bar fuera del rango → pierde 50 puntos.
            Caer 1.0 bar fuera del rango → pierde 100 puntos (score = 0).

        Args:
            value:        Valor actual del sensor.
            optimal_low:  Límite inferior del rango óptimo.
            optimal_high: Límite superior del rango óptimo.

        Returns:
            Puntaje entre 0.0 y 100.0 (redondeado a 2 decimales).
        """
        if optimal_low <= value <= optimal_high:
            return 100.0

        margin = (optimal_high - optimal_low) * 0.5

        if value < optimal_low:
            deviation = (optimal_low - value) / margin
        else:
            deviation = (value - optimal_high) / margin

        return max(0.0, round(100.0 - (deviation * 50), 2))

    def compute(self, row: pd.Series) -> float:
        """
        Calcula el health score global para una lectura del motor (una fila).

        El score final es un promedio ponderado de los puntajes individuales
        de cada sensor. Si algún sensor cae en zona extremadamente crítica
        (puntaje < 20), se aplica una penalización adicional proporcional
        a su peso, para que el score global caiga más bruscamente — reflejando
        que ese sensor solo ya representa un riesgo serio.

        Args:
            row: pd.Series con los valores de los sensores del motor.
                 Puede contener columnas extra; solo se usan las de PARAMETERS.
                 Si falta un sensor, se omite sin error (contribuye 0 al total).

        Returns:
            Score entre 0.0 y 100.0. Cuanto más alto, mejor estado del motor.
        """
        weighted_score = 0.0
        total_weight   = 0.0
        critical_penalty = 0.0

        for param, config in self.PARAMETERS.items():
            if param not in row.index:
                continue
            score = self.compute_parameter_score(
                row[param],
                *config["optimal"]
            )
            weighted_score   += score * config["weight"]
            total_weight     += config["weight"]

            # Penalización extra si el parámetro está en zona crítica
            # (score menor a 20 significa desviación extrema)
            if score < 20:
                critical_penalty += config["weight"] * 40

        if total_weight == 0:
            return 0.0

        base_score = weighted_score / total_weight
        return round(max(0.0, base_score - critical_penalty), 2)

    def add_health_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega dos columnas al DataFrame con el score y el estado del motor.

        Columnas añadidas:
            health_score  — número de 0 a 100 calculado por compute() para
                            cada fila. Permite graficar la evolución del estado
                            del motor a lo largo del tiempo.
            health_status — etiqueta textual (OPTIMAL/GOOD/CAUTION/ALERT/CRITICAL)
                            asignada según los rangos definidos en la clase.

        No modifica el DataFrame original; devuelve una copia nueva.

        Args:
            df: DataFrame con DatetimeIndex y columnas de sensores.
                Salida típica de data_loader.load_engine_data().

        Returns:
            Copia del DataFrame con las dos columnas nuevas añadidas al final.
        """
        df = df.copy()

        df["health_score"] = df.apply(self.compute, axis=1)

        df["health_status"] = pd.cut(
            df["health_score"],
            bins=[0, 40, 60, 75, 90, 100],
            labels=["CRITICAL", "ALERT", "CAUTION", "GOOD", "OPTIMAL"],
            right=True,
            include_lowest=True
        )

        return df

    def get_status_summary(self, df: pd.DataFrame) -> dict:
        """
        Resume la distribución de estados del motor durante todo el período.

        Útil para reportes de guardia: en lugar de revisar 1440 lecturas
        individuales, muestra cuántas horas el motor estuvo en cada estado.

        Args:
            df: DataFrame con columna 'health_status' (salida de add_health_score).

        Returns:
            Diccionario con una clave por estado y dos claves de resumen:
                {
                  "OPTIMAL":  {"count": 1200, "percent": 83.3},
                  "GOOD":     {"count":  180, "percent": 12.5},
                  "CAUTION":  {"count":   50, "percent":  3.5},
                  "ALERT":    {"count":   10, "percent":  0.7},
                  "CRITICAL": {"count":    0, "percent":  0.0},
                  "avg_score": 91.4,   # score promedio del período
                  "min_score": 38.2,   # peor lectura del período
                }

        Raises:
            ValueError: Si el DataFrame no tiene columna 'health_status'.
                        Solución: llamar a add_health_score() primero.
        """
        if "health_status" not in df.columns:
            raise ValueError("Ejecuta add_health_score() primero")

        counts = df["health_status"].value_counts()
        total  = len(df)

        summary = {}
        for status in ["OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"]:
            count = counts.get(status, 0)
            summary[status] = {
                "count":   int(count),
                "percent": round(count / total * 100, 1)
            }

        summary["avg_score"] = round(df["health_score"].mean(), 2)
        summary["min_score"] = round(df["health_score"].min(), 2)

        return summary