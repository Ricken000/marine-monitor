"""
run_pipeline.py — Pipeline principal del sistema de monitoreo marino.

¿Qué hace este script?
    Ejecuta la cadena completa de procesamiento de un extremo al otro:

        [1] SIMULAR   → Genera datos sintéticos de sensores del motor
                        (equivalente a una guardia real de máquinas).
        [2] CARGAR    → Lee el CSV generado y lo prepara para análisis.
        [3] DETECTAR  → Compara cada lectura con el comportamiento normal
                        y marca las que son estadísticamente anómalas.
        [4] PUNTUAR   → Calcula el health score (0–100) para cada lectura
                        y clasifica el estado del motor en 5 niveles.

    Al terminar, guarda dos archivos CSV en el disco:
        data/raw/engine_data_<timestamp>.csv        — lecturas crudas
        data/processed/processed_engine_data_*.csv  — lecturas con scores

¿Cuándo usar este script?
    - Para generar un dataset completo en un solo comando.
    - Para verificar que todos los módulos funcionan integrados.
    - Como punto de entrada de un job automatizado (cron, CI, etc.).

Uso desde la carpeta marine-monitor/:
    python -m src.pipeline.run_pipeline                         # 24h, 3% fallas
    python -m src.pipeline.run_pipeline --hours 48              # 48h de datos
    python -m src.pipeline.run_pipeline --hours 24 --fault-prob 0.05  # 5% fallas

Dependencias internas:
    src.simulator.engine_simulator  → generación de datos
    src.analysis.data_loader        → carga y preparación
    src.analysis.anomaly_detector   → detección estadística de anomalías
    src.analysis.health_score       → índice de salud del motor
"""
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.simulator.engine_simulator import MarineEngineSimulator
from src.analysis.data_loader import load_engine_data, get_summary
from src.analysis.anomaly_detector import StatisticalAnomalyDetector
from src.analysis.health_score import EngineHealthScorer

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


def run(
    hours: int        = 24,
    fault_prob: float = 0.03,
    output_dir: str   = "data/raw/"
) -> dict:
    """
    Ejecuta los 4 pasos del pipeline y devuelve un resumen de métricas.

    Args:
        hours:      Horas de operación a simular (default: 24h = una guardia
                    completa). Con 60s de intervalo genera hours × 60 lecturas.
        fault_prob: Fracción de lecturas que tendrán una falla inyectada
                    (default: 0.03 = 3%). Útil para evaluar la sensibilidad
                    del detector: valores más altos generan más eventos.
        output_dir: Carpeta donde se guarda el CSV de datos crudos.
                    Se crea automáticamente si no existe.

    Returns:
        Diccionario con las métricas clave del pipeline:
            filename            — nombre del CSV de datos crudos generado
            total_readings      — número total de lecturas procesadas
            faults_injected     — fallas inyectadas en la simulación
            critical_anomalies  — anomalías críticas detectadas (z-score ≥ 3)
            avg_health_score    — score promedio del período (0–100)
            min_health_score    — peor score observado en todo el período
            status_distribution — distribución completa de estados del motor
            processed_path      — ruta al CSV procesado con scores
    """

    logger.info("=" * 50)
    logger.info("MARINE ENGINE MONITORING PIPELINE")
    logger.info("=" * 50)

    # ── PASO 1: Simular datos ──────────────────────────────
    logger.info(f"[1/4] Simulando {hours}h de datos...")

    sim = MarineEngineSimulator(seed=42)
    df_raw = sim.generate_dataset(
        hours=hours,
        interval_seconds=60,
        fault_probability=fault_prob
    )

    timestamp   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename    = f"engine_data_{timestamp}.csv"
    output_path = Path(output_dir) / filename
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    df_raw.to_csv(output_path, index=False)

    logger.info(f"    Lecturas generadas : {len(df_raw)}")
    logger.info(f"    Fallas inyectadas  : {df_raw['fault_injected'].sum()}")
    logger.info(f"    Archivo guardado   : {output_path}")

    # ── PASO 2: Cargar y preparar ──────────────────────────
    logger.info("[2/4] Cargando y preparando datos...")

    df = load_engine_data(str(output_path))
    summary = get_summary(df)

    logger.info(f"    Período : {summary['start_time'][:19]} "
                f"→ {summary['end_time'][:19]}")
    logger.info(f"    Duración: {summary['duration_hours']}h")

    # ── PASO 3: Detectar anomalías ─────────────────────────
    logger.info("[3/4] Detectando anomalías...")

    # Primeras 6h como línea base (asumidas normales)
    baseline_size = min(360, len(df) // 4)
    baseline_df   = df.iloc[:baseline_size]

    detector = StatisticalAnomalyDetector(
        warning_threshold=2.0,
        critical_threshold=3.0
    )
    detector.fit(baseline_df)

    df_analyzed, anomalies = detector.detect(df)

    critical_anomalies = [a for a in anomalies if a.severity == "critical"]
    warning_anomalies  = [a for a in anomalies if a.severity == "warning"]

    logger.info(f"    Anomalías críticas    : {len(critical_anomalies)}")
    logger.info(f"    Advertencias          : {len(warning_anomalies)}")

    # ── PASO 4: Health Score ───────────────────────────────
    logger.info("[4/4] Calculando health scores...")

    scorer    = EngineHealthScorer()
    df_final  = scorer.add_health_score(df_analyzed)
    hs_summary = scorer.get_status_summary(df_final)

    logger.info(f"    Score promedio : {hs_summary['avg_score']}")
    logger.info(f"    Score mínimo   : {hs_summary['min_score']}")

    # ── REPORTE FINAL ──────────────────────────────────────
    logger.info("=" * 50)
    logger.info("RESULTADO FINAL")
    logger.info("=" * 50)

    for status in ["OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"]:
        data = hs_summary[status]
        if data["count"] > 0:
            logger.info(
                f"    {status:<10} {data['count']:>5} lecturas "
                f"({data['percent']:>5.1f}%)"
            )

    # Guardar CSV procesado
    processed_dir  = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    processed_path = processed_dir / f"processed_{filename}"
    df_final.to_csv(processed_path)

    logger.info(f"\n    CSV procesado: {processed_path}")
    logger.info("=" * 50)

    # ── RETORNAR MÉTRICAS ──────────────────────────────────
    return {
        "filename":         filename,
        "total_readings":   len(df_final),
        "faults_injected":  int(df_raw["fault_injected"].sum()),
        "critical_anomalies": len(critical_anomalies),
        "avg_health_score": hs_summary["avg_score"],
        "min_health_score": hs_summary["min_score"],
        "status_distribution": hs_summary,
        "processed_path":   str(processed_path),
    }


# ── ENTRY POINT ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Marine Engine Monitoring Pipeline"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Horas de datos a simular (default: 24)"
    )
    parser.add_argument(
        "--fault-prob",
        type=float,
        default=0.03,
        help="Probabilidad de falla por lectura (default: 0.03)"
    )
    args = parser.parse_args()

    results = run(
        hours=args.hours,
        fault_prob=args.fault_prob
    )

    print("\nMétricas del pipeline:")
    for key, value in results.items():
        if key != "status_distribution":
            print(f"  {key}: {value}")