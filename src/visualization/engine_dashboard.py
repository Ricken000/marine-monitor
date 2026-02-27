"""
engine_dashboard.py — Generador de reportes visuales del motor marino.

¿Qué hace este módulo?
    Toma el DataFrame procesado (con health_score, health_status y lecturas
    de sensores) y produce un archivo HTML interactivo listo para abrir en
    cualquier navegador, sin necesidad de un servidor web.

Estructura del reporte generado:
    [Header]   Período analizado y versión del sistema.
    [KPIs]     4 tarjetas: score promedio, score mínimo, total de lecturas
               y número de eventos de falla. El color del score cambia según
               el nivel (verde = OPTIMAL, naranja = CAUTION, rojo = CRITICAL).
    [Gráfico 1 — Engine Health Score]
               Línea del score (0–100) a lo largo del tiempo con líneas
               horizontales punteadas que marcan los umbrales de cada nivel.
    [Gráfico 2 — Engine Parameters 24h Monitor]
               6 subplots, uno por sensor. Cada subplot tiene una banda verde
               semitransparente que indica el rango óptimo de ese sensor.
               Los ejes Y tienen mínimos personalizados por sensor para que
               la variación sea visualmente legible.
    [Gráfico 3 — Operational Status Distribution]
               Barras por estado (OPTIMAL / GOOD / CAUTION / ALERT / CRITICAL)
               con recuento y porcentaje sobre cada barra.

Tecnología:
    Plotly (gráficos interactivos embebidos como JavaScript en el HTML).
    El primer gráfico carga Plotly.js desde CDN; los otros dos lo reutilizan,
    por lo que el archivo HTML funciona offline solo si se cargó una vez con
    conexión a internet.

Uso típico:
    from src.visualization.engine_dashboard import generate_report
    generate_report(df_final, "reports/engine_report.html")

Dependencias: plotly, pandas, pathlib (stdlib)
"""
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from pathlib import Path

# ── Constantes de diseño ───────────────────────────────────────────────────
COLORS = {
    "OPTIMAL":  "#2ecc71",
    "GOOD":     "#27ae60",
    "CAUTION":  "#f39c12",
    "ALERT":    "#e67e22",
    "CRITICAL": "#e74c3c",
    "normal":   "#3498db",
    "fault":    "#e74c3c",
}

OPTIMAL_RANGES = {
    "rpm":                 (90, 100),
    "temperature_exhaust": (330, 370),
    "temperature_cooling": (72, 82),
    "pressure_lube":       (3.8, 4.8),
    "pressure_fuel":       (8.5, 9.5),
    "vibration_rms":       (1.5, 3.0),
}


def _add_range_bands(
    fig: go.Figure,
    param: str,
    row: int,
    col: int
) -> None:
    """
    Pinta una banda verde semitransparente sobre el rango óptimo del sensor.

    La banda ayuda a identificar visualmente de un vistazo si las lecturas
    se mantienen dentro del rango normal o se alejan de él. Si el parámetro
    no tiene un rango definido en OPTIMAL_RANGES, la función no hace nada.

    Args:
        fig:   Figura de subplots donde se pintará la banda.
        param: Nombre del sensor (debe existir en OPTIMAL_RANGES).
        row:   Fila del subplot (1-indexado).
        col:   Columna del subplot (1-indexado).
    """
    if param not in OPTIMAL_RANGES:
        return
    low, high = OPTIMAL_RANGES[param]
    fig.add_hrect(
        y0=low, y1=high,
        fillcolor="rgba(46, 204, 113, 0.1)",
        line_width=1,
        row=row, col=col  # type: ignore[arg-type]
    )


def create_health_score_chart(df: pd.DataFrame) -> go.Figure:
    """
    Crea el gráfico de línea del health score a lo largo del tiempo.

    Muestra la evolución del índice de salud (0–100) con líneas horizontales
    punteadas que marcan los umbrales de cada nivel operacional, facilitando
    identificar en qué momentos el motor bajó de zona segura.

    Args:
        df: DataFrame con columna 'health_score' e índice de tipo DatetimeIndex.

    Returns:
        Figura Plotly lista para incrustar en HTML con .to_html().
    """
    fig = go.Figure()

    # Línea del health score
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df["health_score"],
        mode="lines",
        line=dict(color=COLORS["normal"], width=1.5),
        name="Health Score",
        hovertemplate="<b>%{y:.1f}/100</b><br>%{x}<extra></extra>"
    ))

    # Líneas de umbral
    thresholds = [
        (90, COLORS["OPTIMAL"],  "Optimal"),
        (75, COLORS["GOOD"],     "Good"),
        (60, COLORS["CAUTION"],  "Caution"),
        (40, COLORS["ALERT"],    "Alert"),
    ]
    for y, color, label in thresholds:
        fig.add_hline(
            y=y,
            line_dash="dash",
            line_color=color,
            line_width=1,
            annotation_text=label,
            annotation_position="right"
        )

    fig.update_layout(
        title="Engine Health Score",
        yaxis=dict(range=[0, 105], title="Score"),
        template="plotly_dark",
        height=300,
        showlegend=False,
        margin=dict(l=60, r=100, t=40, b=40)
    )
    return fig


def create_parameters_chart(df: pd.DataFrame) -> go.Figure:
    """
    Crea un panel de 6 subplots, uno por cada sensor del motor.

    Cada subplot muestra la serie temporal del sensor con:
      - Una banda verde semitransparente sobre el rango óptimo del sensor.
      - Eje Y con mínimo personalizado para algunos sensores (temperaturas
        y presiones) de modo que la variación sea visualmente legible en
        lugar de comenzar siempre desde cero.
      - Eje Y que termina 20% por encima del valor máximo registrado.

    Sensores incluidos (en orden de posición en el panel):
        Fila 1: RPM | Exhaust Temp (°C)
        Fila 2: Cooling Temp (°C) | Lube Pressure (bar)
        Fila 3: Fuel Pressure (bar) | Vibration (mm/s)

    Args:
        df: DataFrame con columnas de sensores e índice DatetimeIndex.
            Debe incluir 'fault_injected' para colorear anomalías (unused
            actualmente — la línea usa siempre color normal).

    Returns:
        Figura Plotly lista para incrustar en HTML con .to_html().
    """
    params = [
        ("rpm",                 "RPM",              1, 1),
        ("temperature_exhaust", "Exhaust Temp (°C)", 1, 2),
        ("temperature_cooling", "Cooling Temp (°C)", 2, 1),
        ("pressure_lube",       "Lube Pressure (bar)", 2, 2),
        ("pressure_fuel",       "Fuel Pressure (bar)", 3, 1),
        ("vibration_rms",       "Vibration (mm/s)",    3, 2),
    ]

    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=[p[1] for p in params],
        vertical_spacing=0.12,
        horizontal_spacing=0.08
    )

    for param, title, row, col in params:
        if param not in df.columns:
            continue

        # Color por estado de falla
        colors = df["fault_injected"].map(
            {True: COLORS["fault"], False: COLORS["normal"]}
        )

        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[param],
                mode="lines",
                line=dict(color=COLORS["normal"], width=1),
                name=title,
                hovertemplate=f"<b>{title}</b><br>%{{y:.2f}}<br>%{{x}}<extra></extra>"
            ),
            row=row, col=col
        )

        # Banda de rango óptimo
        _add_range_bands(fig, param, row, col)

        # Eje Y: por defecto inicia en 0; algunos sensores tienen un mínimo
        # personalizado para que la variación sea visualmente más legible.
        Y_MIN_OVERRIDE = {
            "temperature_exhaust": 150,
            "temperature_cooling": 30,
            "pressure_lube":        1,
            "pressure_fuel":        4,
        }
        y_min = Y_MIN_OVERRIDE.get(param, 0)
        max_val = df[param].max()
        fig.update_yaxes(range=[y_min, max_val * 1.2], row=row, col=col)  # type: ignore[arg-type]

    fig.update_layout(
        title="Engine Parameters — 24h Monitor",
        template="plotly_dark",
        height=700,
        showlegend=False,
        margin=dict(l=60, r=40, t=60, b=40)
    )
    return fig


def create_status_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """
    Crea un gráfico de barras con la distribución de estados operacionales.

    Muestra cuántas lecturas cayeron en cada nivel (OPTIMAL, GOOD, CAUTION,
    ALERT, CRITICAL) durante el período analizado, con recuento absoluto y
    porcentaje sobre cada barra. Cada estado tiene su propio color para
    identificarlo de un vistazo.

    El eje Y se extiende un 40% por encima de la barra más alta para que el
    texto con el porcentaje no quede cortado por el borde del gráfico.

    Args:
        df: DataFrame con columna 'health_status' (salida de add_health_score).

    Returns:
        Figura Plotly lista para incrustar en HTML con .to_html().
    """
    status_order  = ["OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"]
    status_counts = df["health_status"].value_counts()

    counts = [status_counts.get(s, 0) for s in status_order]
    colors = [COLORS[s] for s in status_order]
    total  = len(df)

    fig = go.Figure(go.Bar(
        x=status_order,
        y=counts,
        marker_color=colors,
        text=[f"{c}<br>({c/total*100:.1f}%)" for c in counts],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>%{y} lecturas<extra></extra>"
    ))

    # Eje Y con margen extra para que el texto "outside" de la barra más alta
    # no quede cortado por el borde superior del gráfico
    max_count = max(counts) if counts else 1
    fig.update_layout(
        title="Operational Status Distribution",
        template="plotly_dark",
        height=350,
        showlegend=False,
        yaxis=dict(title="Lecturas", range=[0, max_count * 1.4]),
        margin=dict(l=60, r=40, t=40, b=40)
    )
    return fig


def generate_report(
    df: pd.DataFrame,
    output_path: str = "reports/engine_report.html"
) -> str:
    """
    Genera el reporte HTML completo y lo guarda en disco.

    Combina los tres gráficos (health score, parámetros, distribución) en
    una página HTML con fondo oscuro, tarjetas de KPIs en el header y los
    gráficos interactivos de Plotly embebidos directamente en el archivo.

    El reporte no requiere servidor web: basta con abrir el HTML en el
    navegador. Plotly.js se carga desde CDN en el primer gráfico; los otros
    dos lo reutilizan, así que el archivo funciona offline solo después de
    haberlo abierto al menos una vez con conexión.

    Args:
        df:          DataFrame procesado con columnas 'health_score',
                     'health_status', 'fault_injected' y los sensores del
                     motor. Salida típica de scorer.add_health_score().
        output_path: Ruta del archivo HTML a generar. La carpeta se crea
                     automáticamente si no existe.

    Returns:
        Ruta absoluta o relativa del archivo HTML generado (igual a output_path).
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Generar los tres gráficos
    fig_health = create_health_score_chart(df)
    fig_params = create_parameters_chart(df)
    fig_status = create_status_distribution_chart(df)

    # Métricas para el header
    avg_score    = df["health_score"].mean()
    min_score    = df["health_score"].min()
    fault_count  = df["fault_injected"].sum()
    total        = len(df)
    period_start = str(df.index.min())[:19]
    period_end   = str(df.index.max())[:19]

    # Color del score promedio
    if avg_score >= 90:
        score_color = COLORS["OPTIMAL"]
    elif avg_score >= 75:
        score_color = COLORS["GOOD"]
    elif avg_score >= 60:
        score_color = COLORS["CAUTION"]
    else:
        score_color = COLORS["CRITICAL"]

    # HTML completo
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Marine Engine — Monitoring Report</title>
    <style>
        body {{
            background-color: #1a1a2e;
            color: #eee;
            font-family: 'Segoe UI', sans-serif;
            margin: 0;
            padding: 20px;
        }}
        .header {{
            text-align: center;
            padding: 30px 0 20px 0;
            border-bottom: 1px solid #333;
            margin-bottom: 30px;
        }}
        .header h1 {{
            font-size: 1.8em;
            letter-spacing: 2px;
            color: #3498db;
            margin: 0 0 5px 0;
        }}
        .header p {{
            color: #888;
            margin: 4px 0;
            font-size: 0.9em;
        }}
        .kpis {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin: 25px 0;
            flex-wrap: wrap;
        }}
        .kpi {{
            background: #16213e;
            border-radius: 8px;
            padding: 15px 25px;
            text-align: center;
            min-width: 130px;
        }}
        .kpi .value {{
            font-size: 2em;
            font-weight: bold;
        }}
        .kpi .label {{
            font-size: 0.75em;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-top: 4px;
        }}
        .chart-container {{
            background: #16213e;
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>⚙ MARINE ENGINE MONITORING REPORT</h1>
        <p>Period: {period_start} → {period_end}</p>
        <p>Generated by Marine Engine Monitoring System v1.0</p>
    </div>

    <div class="kpis">
        <div class="kpi">
            <div class="value" style="color: {score_color}">
                {avg_score:.1f}
            </div>
            <div class="label">Avg Health Score</div>
        </div>
        <div class="kpi">
            <div class="value" style="color: {COLORS['CAUTION']}">
                {min_score:.1f}
            </div>
            <div class="label">Min Health Score</div>
        </div>
        <div class="kpi">
            <div class="value">{total}</div>
            <div class="label">Total Readings</div>
        </div>
        <div class="kpi">
            <div class="value" style="color: {COLORS['ALERT']}">
                {fault_count}
            </div>
            <div class="label">Fault Events</div>
        </div>
    </div>

    <div class="chart-container">
        {fig_health.to_html(full_html=False, include_plotlyjs='cdn')}
    </div>

    <div class="chart-container">
        {fig_params.to_html(full_html=False, include_plotlyjs=False)}
    </div>

    <div class="chart-container">
        {fig_status.to_html(full_html=False, include_plotlyjs=False)}
    </div>

</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Report generated: {output_path}")
    return output_path