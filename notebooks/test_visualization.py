"""
test_visualization.py — Generación end-to-end del reporte HTML del motor.

¿Qué hace este script?
    Ejecuta la cadena completa de procesamiento y al finalizar abre el reporte
    en el navegador predeterminado del sistema, permitiendo revisar el resultado
    de forma inmediata sin pasos adicionales.

    Pasos internos:
        [1] Carga el CSV histórico de lecturas del motor.
        [2] Entrena el detector de anomalías con las primeras 6 horas (360
            lecturas), asumidas como período de operación normal.
        [3] Aplica el detector sobre todo el dataset para marcar anomalías.
        [4] Calcula el health score (0–100) y el estado operacional por lectura.
        [5] Genera el reporte HTML con tres gráficos interactivos y lo abre
            automáticamente en el navegador.

Resultado esperado:
    Se abre en el navegador un reporte con fondo oscuro que incluye:
      - 4 KPIs: score promedio, score mínimo, total de lecturas, fallas.
      - Gráfico del health score en el tiempo con umbrales por nivel.
      - Panel de 6 subplots con la evolución de cada sensor.
      - Gráfico de barras con la distribución de estados operacionales.

Cómo ejecutarlo (desde marine-monitor/):
    python notebooks/test_visualization.py

Requisito previo:
    El CSV data/raw/engine_readings_24h.csv debe existir. Generarlo con:
        python -m src.simulator.engine_simulator
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.data_loader import load_engine_data
from src.analysis.anomaly_detector import StatisticalAnomalyDetector
from src.analysis.health_score import EngineHealthScorer
from src.visualization.engine_dashboard import generate_report
import os

# Cargar y procesar datos
DATA_PATH = Path(__file__).parent.parent / "data" / "raw" / "engine_readings_24h.csv"
df = load_engine_data(str(DATA_PATH))

baseline_df = df.iloc[:360]
detector    = StatisticalAnomalyDetector()
detector.fit(baseline_df)
df_analyzed, _ = detector.detect(df)

scorer   = EngineHealthScorer()
df_final = scorer.add_health_score(df_analyzed)

# Generar reporte
REPORT_PATH = Path(__file__).parent.parent / "reports" / "engine_report.html"
output = generate_report(df_final, str(REPORT_PATH))

# Abrir en el navegador automáticamente
os.startfile(output)