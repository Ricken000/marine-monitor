"""
anomaly_detector.py — Detección de anomalías estadística para motores marinos.

Algoritmo: z-score sobre línea base estática (media y std del período normal).

Cómo funciona el z-score en este contexto:
  El z-score mide cuántas desviaciones estándar se aleja una lectura del sensor
  respecto al comportamiento promedio durante operación normal. Por ejemplo:
    z = (valor_actual - media_normal) / std_normal
  Un z-score de 2.0 significa que el valor está 2 desviaciones estándar por
  encima o por debajo del promedio histórico — señal de posible anomalía.
  Un z-score de 3.0 se considera crítico (regla empírica del 99.7%).

Flujo de uso esperado:
  1. Cargar datos históricos con data_loader.load_engine_data()
  2. Crear instancia: detector = StatisticalAnomalyDetector()
  3. Entrenar: detector.fit(df)   ← solo con datos de operación normal
  4. Detectar: df_result, anomalies = detector.detect(df_nuevo)
  5. Reportar: detector.print_report(anomalies)

Limitaciones conocidas:
  - La línea base es estática: si el motor envejece gradualmente, la media
    base quedará desactualizada y generará falsos positivos. Para producción
    considerar un baseline adaptativo con ventana deslizante.
  - No distingue entre anomalías transitorias (pico puntual) y sostenidas
    (degradación prolongada). El detector de tendencias es responsabilidad
    de otro módulo.

Dependencias: pandas, numpy, dataclasses (stdlib)
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass


@dataclass
class AnomalyResult:
    """
    Representa una anomalía individual detectada en un sensor.

    Campos:
        parameter:  nombre del sensor afectado (ej. 'temperature_exhaust')
        timestamp:  instante de la anomalía en formato ISO 8601 (string)
        value:      valor real registrado por el sensor en ese instante
        zscore:     distancia en desviaciones estándar respecto a la media normal
        severity:   'warning' (z >= 2.0) o 'critical' (z >= 3.0)
    """
    parameter:  str
    timestamp:  str
    value:      float
    zscore:     float
    severity:   str  # 'warning' | 'critical'


class StatisticalAnomalyDetector:
    """
    Detector de anomalías basado en z-score con línea base histórica.

    La detección se divide en dos fases explícitas para poder reutilizar
    el mismo modelo entrenado sobre múltiples datasets sin recalcular:

        fit()    — aprende el comportamiento normal desde datos históricos
        detect() — evalúa nuevos datos contra esa línea base aprendida

    Umbrales por defecto (configurables en __init__):
        warning_threshold  = 2.0  →  cubre ~95.4% de operación normal
        critical_threshold = 3.0  →  cubre ~99.7% de operación normal
    Cualquier lectura fuera de esos rangos se marca como anomalía.
    """

    # Lista canónica de sensores del motor. Centralizada aquí para que
    # fit(), detect() y print_report() estén siempre sincronizados.
    # Si se agrega un sensor nuevo al simulador, basta añadirlo aquí.
    PARAMS = [
        "rpm", "temperature_exhaust", "temperature_cooling",
        "pressure_lube", "pressure_fuel", "vibration_rms"
    ]

    def __init__(
        self,
        warning_threshold:  float = 2.0,
        critical_threshold: float = 3.0,
        rolling_window:     str   = "1h"
    ):
        """
        Args:
            warning_threshold:  z-score mínimo para clasificar como 'warning'.
            critical_threshold: z-score mínimo para clasificar como 'critical'.
            rolling_window:     reservado para uso futuro en baseline adaptativo.
        """
        self.warning_threshold  = warning_threshold
        self.critical_threshold = critical_threshold
        self.rolling_window     = rolling_window
        # Dict {param: {"mean": float, "std": float}} — se llena en fit().
        # Vacío antes de fit(); detect() lanza RuntimeError si está vacío.
        self.baselines          = {}

    def fit(self, df: pd.DataFrame) -> None:
        """
        Calcula la línea base (media y std) desde datos de operación normal.

        Por qué filtrar solo las filas sin falla:
          Si se incluyen datos de fallas en el entrenamiento, la media y std
          se desplazan hacia valores anómalos, reduciendo la sensibilidad del
          detector. La línea base debe representar el comportamiento saludable.

        Args:
            df: DataFrame completo con columna 'fault_injected' (0=normal, 1=falla).
                Debe incluir los sensores listados en PARAMS.
        """
        # Usamos solo las lecturas etiquetadas como operación normal.
        normal_df = df[df["fault_injected"] == False]

        for param in self.PARAMS:
            # Tolerancia ante datasets sin ese sensor (versiones antiguas del CSV).
            if param not in normal_df.columns:
                continue
            self.baselines[param] = {
                "mean": normal_df[param].mean(),
                "std":  normal_df[param].std(),
            }

        print(f"Detector entrenado con {len(normal_df)} lecturas normales")

    def _compute_zscore(self, value: float, param: str) -> float:
        """
        Calcula el z-score de una lectura individual contra la línea base.

        Casos especiales:
          - Si param no fue entrenado (no está en baselines): retorna 0.0
            para no generar falsos positivos en sensores desconocidos.
          - Si std == 0 (sensor con valor constante en datos de entrenamiento):
            retorna 0.0 para evitar división por cero.

        Args:
            value: Lectura actual del sensor.
            param: Nombre del sensor (debe coincidir con una clave en PARAMS).

        Returns:
            z-score como float. Puede ser negativo (valor bajo) o positivo (alto).
        """
        baseline = self.baselines.get(param)
        if baseline is None or baseline["std"] == 0:
            return 0.0
        return (value - baseline["mean"]) / baseline["std"]

    def _classify_severity(self, zscore: float) -> str | None:
        """
        Convierte un z-score numérico en una etiqueta de severidad.

        Usa el valor absoluto del z-score para detectar tanto desviaciones
        por encima (ej. temperatura alta) como por debajo (ej. presión baja).

        Args:
            zscore: z-score calculado por _compute_zscore().

        Returns:
            'critical' si |z| >= critical_threshold,
            'warning'  si |z| >= warning_threshold,
            None       si el valor está dentro del rango normal.
        """
        abs_z = abs(zscore)
        if abs_z >= self.critical_threshold:
            return "critical"
        elif abs_z >= self.warning_threshold:
            return "warning"
        return None

    def detect(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[AnomalyResult]]:
        """
        Evalúa cada lectura del dataset y marca las anomalías encontradas.

        Genera dos outputs complementarios:
          - df_result: útil para visualización (graficar z-scores en el tiempo).
          - anomalies: útil para alertas y reportes (lista de eventos puntuales).

        Args:
            df: DataFrame con DatetimeIndex y columnas de sensores.
                No necesita estar filtrado; se evalúan todas las filas.

        Returns:
            df_result:  Copia de df con columnas adicionales por cada sensor:
                          '<param>_zscore'  — z-score de cada lectura (float)
                          '<param>_anomaly' — True si supera warning_threshold
            anomalies:  Lista de AnomalyResult, una entrada por cada lectura
                        que superó el warning_threshold en cualquier sensor.

        Raises:
            RuntimeError: Si se llama antes de fit().
        """
        if not self.baselines:
            raise RuntimeError("Debes llamar fit() antes de detect()")

        df_result = df.copy()
        anomalies = []

        for param in self.PARAMS:
            if param not in df.columns:
                continue

            # Calculamos los z-scores de toda la columna de una vez con .apply()
            # en lugar de un loop fila por fila, por eficiencia con pandas.
            zscores = df[param].apply(
                lambda val: self._compute_zscore(val, param)
            )

            # Columna de z-score para graficar la evolución temporal del sensor.
            df_result[f"{param}_zscore"]  = zscores.round(3)
            # Columna booleana: True indica que esa lectura es potencialmente anómala.
            df_result[f"{param}_anomaly"] = zscores.abs() >= self.warning_threshold

            # Segundo recorrido para construir la lista de eventos individuales.
            # Separado del bloque anterior porque AnomalyResult necesita clasificar
            # en warning vs critical, no solo marcar si es anómalo o no.
            for timestamp, row in df.iterrows():
                z = self._compute_zscore(row[param], param)
                severity = self._classify_severity(z)

                if severity:
                    anomalies.append(AnomalyResult(
                        parameter=param,
                        timestamp=str(timestamp),
                        value=round(row[param], 2),
                        zscore=round(z, 3),
                        severity=severity,
                    ))

        return df_result, anomalies

    def print_report(self, anomalies: list[AnomalyResult]) -> None:
        """
        Imprime en consola un resumen estructurado de las anomalías detectadas.

        Diseñado para inspección rápida durante desarrollo o en logs del servidor.
        Para reportes persistentes, exportar la lista 'anomalies' directamente.

        Muestra:
          - Totales por severidad (críticas y advertencias).
          - Conteo y z-score máximo por sensor.
          - Detalle de las primeras 5 anomalías críticas (las más graves primero).

        Args:
            anomalies: Lista devuelta por detect(). Puede estar vacía.
        """
        if not anomalies:
            print("Sin anomalías detectadas")
            return

        critical = [a for a in anomalies if a.severity == "critical"]
        warnings  = [a for a in anomalies if a.severity == "warning"]

        print("\n" + "=" * 55)
        print("REPORTE DE ANOMALÍAS")
        print("=" * 55)
        print(f"Total anomalías : {len(anomalies)}")
        print(f"  Críticas      : {len(critical)}")
        print(f"  Advertencias  : {len(warnings)}")

        print("\n-- Por parametro --")
        for param in self.PARAMS:
            param_anomalies = [a for a in anomalies if a.parameter == param]
            if param_anomalies:
                max_z = max(abs(a.zscore) for a in param_anomalies)
                print(f"  {param:<25} {len(param_anomalies):>4} anomalias  "
                      f"max z={max_z:.2f}")

        # Se muestran solo las primeras 5 para no saturar la consola.
        # Si hay más, se indica el total restante al final.
        print("\n-- Criticas (z >= 3.0) --")
        for a in critical[:5]:
            print(f"  {a.parameter:<25} valor={a.value:>8.2f}  "
                  f"z={a.zscore:>6.3f}  {a.timestamp[:19]}")

        if len(critical) > 5:
            print(f"  ... y {len(critical) - 5} más")

        print("=" * 55)
