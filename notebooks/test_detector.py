"""
test_detector.py — Validación manual del módulo StatisticalAnomalyDetector.

¿Qué verifica este script?
    Comprueba que el detector identifica correctamente las fallas inyectadas
    en el dataset histórico usando una división entrenamiento / evaluación:

        Entrenamiento (primeras 6h = 360 lecturas):
            El detector aprende cómo se comporta el motor cuando está sano.
            Calcula la media y desviación estándar de cada sensor en ese período.

        Evaluación (restantes 18h = 1080 lecturas):
            El detector analiza el resto del dataset buscando lecturas que se
            alejen significativamente del comportamiento aprendido.
            Se usa un umbral de 2 desviaciones estándar para 'warning' y
            3 desviaciones para 'critical'.

    Al final se compara:
        - Fallas inyectadas en el período de evaluación (etiqueta real).
        - Anomalías críticas detectadas por el modelo (predicción).
    Esa comparación permite evaluar si el detector tiene buena cobertura.

    Resultado esperado con fault_prob=0.03 y 18h de evaluación:
        ~32 fallas inyectadas, mayoría detectadas como anomalías críticas.

Cómo ejecutarlo (desde marine-monitor/):
    python notebooks/test_detector.py

Requisito previo:
    El CSV data/raw/engine_readings_24h.csv debe existir. Generarlo con:
        python -m src.simulator.engine_simulator
"""
import sys
from pathlib import Path

# Agrega marine-monitor/ al path de Python para que 'src' sea importable
# al ejecutar el script directamente con `python notebooks/test_detector.py`.
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.data_loader import load_engine_data
from src.analysis.anomaly_detector import StatisticalAnomalyDetector

# Ruta absoluta al CSV, independiente del directorio de trabajo actual
DATA_PATH = Path(__file__).parent.parent / "data" / "raw" / "engine_readings_24h.csv"
df = load_engine_data(str(DATA_PATH))

# Separar primeras 6h como línea base
baseline_df = df.iloc[:360]
eval_df     = df.iloc[360:]

print(f'Entrenamiento : {len(baseline_df)} lecturas (6h)')
print(f'Evaluación    : {len(eval_df)} lecturas (18h)')

# Entrenar y detectar
detector = StatisticalAnomalyDetector(
    warning_threshold=2.0,
    critical_threshold=3.0
)
detector.fit(baseline_df)

df_result, anomalies = detector.detect(eval_df)
detector.print_report(anomalies)

# Verificación cruzada
faults_in_eval = eval_df['fault_injected'].sum()
critical_found = [a for a in anomalies if a.severity == 'critical']

print(f'\nFallas inyectadas en evaluación : {faults_in_eval}')
print(f'Anomalías críticas detectadas   : {len(critical_found)}')