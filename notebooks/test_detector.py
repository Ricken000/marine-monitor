import sys
from pathlib import Path

# Agrega la raíz del proyecto (marine-monitor/) al path de Python.
# Necesario porque los scripts en notebooks/ no tienen visibilidad de src/
# cuando se ejecutan directamente con `python notebooks/test_detector.py`.
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.data_loader import load_engine_data
from src.analysis.anomaly_detector import StatisticalAnomalyDetector

# Cargar datos
df = load_engine_data('data/raw/engine_readings_24h.csv')

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