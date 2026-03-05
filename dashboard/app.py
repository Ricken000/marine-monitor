"""
Interactive Streamlit dashboard for the Marine Engine Predictive Monitoring System.

Provides a real-time visual interface over the analysis pipeline, allowing
engineers to explore engine health, sensor trends, and anomaly distribution
without writing any code.

Key features:
    - Configurable simulation parameters (seed, duration, fault rate) with
      synchronized slider + numeric input controls
    - Health Score Timeline with severity threshold reference lines
    - Six-panel Engine Parameters chart with calibrated y-axis ranges per sensor
    - Status Distribution bar chart across all five severity levels
    - Recent Alerts table showing the latest CAUTION/ALERT/CRITICAL readings
    - One-click CSV export of the full processed dataset

Run from the project root:
    streamlit run dashboard/app.py
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sys
import os

# Ensure the project root is on the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.simulator.engine_simulator import MarineEngineSimulator
from src.analysis.anomaly_detector import StatisticalAnomalyDetector
from src.analysis.health_score import EngineHealthScorer

# ── Page configuration ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="Marine Engine Monitor",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Styles ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: #1a1f2e;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
    }
    .stMetric label { font-size: 0.75em !important; }
</style>
""", unsafe_allow_html=True)

COLORS = {
    "OPTIMAL":  "#2ecc71",
    "GOOD":     "#27ae60",
    "CAUTION":  "#f39c12",
    "ALERT":    "#e67e22",
    "CRITICAL": "#e74c3c",
    "line":     "#3498db",
}


# ── Analysis functions ─────────────────────────────────────────────────────
@st.cache_data
def run_analysis(seed: int, hours: int, fault_prob: float):
    """
    Run the full analysis pipeline and cache the result.

    The @st.cache_data decorator prevents re-execution when the same
    parameters are used, keeping the UI responsive on re-renders.
    """
    # Simulate
    sim    = MarineEngineSimulator(seed=seed)
    df_raw = sim.generate_dataset(
        hours=hours,
        interval_seconds=60,
        fault_probability=fault_prob
    )
    df_raw["timestamp"] = pd.to_datetime(df_raw["timestamp"])
    df_raw.set_index("timestamp", inplace=True)

    # Detect anomalies
    baseline_size = min(360, len(df_raw) // 4)
    baseline_df   = df_raw.iloc[:baseline_size]
    detector      = StatisticalAnomalyDetector(
        warning_threshold=2.0,
        critical_threshold=3.0
    )
    detector.fit(baseline_df)
    df_analyzed, anomalies = detector.detect(df_raw)

    # Health score
    scorer   = EngineHealthScorer()
    df_final = scorer.add_health_score(df_analyzed)

    return df_final, anomalies


def get_status_color(score: float) -> str:
    if score >= 90: return COLORS["OPTIMAL"]
    if score >= 75: return COLORS["GOOD"]
    if score >= 60: return COLORS["CAUTION"]
    if score >= 40: return COLORS["ALERT"]
    return COLORS["CRITICAL"]


def get_status_label(score: float) -> str:
    if score >= 90: return "OPTIMAL"
    if score >= 75: return "GOOD"
    if score >= 60: return "CAUTION"
    if score >= 40: return "ALERT"
    return "CRITICAL"


# ── Sidebar ────────────────────────────────────────────────────────────────
# Initialize session state BEFORE widget calls to avoid the
# "default value + Session State API" conflict warning.
if "seed_slider" not in st.session_state:
    st.session_state.seed_slider  = 42
    st.session_state.seed_input   = 42
if "hours_slider" not in st.session_state:
    st.session_state.hours_slider = 24
    st.session_state.hours_input  = 24
if "fault_slider" not in st.session_state:
    st.session_state.fault_slider = 3    # integer percent: 3 = 3%
    st.session_state.fault_input  = 3

with st.sidebar:
    st.title("⚙️ Marine Engine\nMonitor")
    st.divider()

    st.subheader("⚙️ Simulation Parameters")

    # Bidirectional sync: slider ↔ number input via on_change callbacks.
    def _on_seed_slider():  st.session_state.seed_input   = st.session_state.seed_slider
    def _on_seed_input():   st.session_state.seed_slider  = st.session_state.seed_input
    def _on_hours_slider(): st.session_state.hours_input  = st.session_state.hours_slider
    def _on_hours_input():  st.session_state.hours_slider = st.session_state.hours_input
    def _on_fault_slider(): st.session_state.fault_input  = st.session_state.fault_slider
    def _on_fault_input():  st.session_state.fault_slider = st.session_state.fault_input

    st.caption("Random Seed")
    c1, c2 = st.columns([3, 1])
    with c1:
        st.slider("Random Seed", 0, 999, key="seed_slider",
                  on_change=_on_seed_slider, label_visibility="collapsed")
    with c2:
        st.number_input("Random Seed", 0, 999, step=1, key="seed_input",
                        on_change=_on_seed_input, label_visibility="collapsed")
    seed = st.session_state.seed_slider

    st.caption("Hours to simulate")
    c1, c2 = st.columns([3, 1])
    with c1:
        st.slider("Hours to simulate", 6, 72, key="hours_slider",
                  on_change=_on_hours_slider, label_visibility="collapsed")
    with c2:
        st.number_input("Hours to simulate", 6, 72, step=1, key="hours_input",
                        on_change=_on_hours_input, label_visibility="collapsed")
    hours = st.session_state.hours_slider

    st.caption("Fault probability (%)")
    c1, c2 = st.columns([3, 1])
    with c1:
        st.slider("Fault probability (%)", 0, 15, key="fault_slider",
                  on_change=_on_fault_slider,
                  help="Percentage of readings that will have an injected fault",
                  label_visibility="collapsed")
    with c2:
        st.number_input("Fault probability (%)", 0, 15, step=1, key="fault_input",
                        on_change=_on_fault_input, label_visibility="collapsed")
    fault_prob = st.session_state.fault_slider / 100.0

    run_btn = st.button(
        "▶ Run Analysis",
        type="primary",
        use_container_width=True
    )

    st.divider()
    st.caption("Marine Engine Monitoring System v1.0")
    st.caption("Built with Python + AWS")

# ── Main content ───────────────────────────────────────────────────────────
st.title("⚙️ Marine Engine — Predictive Monitoring System")

if run_btn or "df" not in st.session_state:
    with st.spinner("Running analysis pipeline..."):
        df, anomalies = run_analysis(seed, hours, fault_prob)
        st.session_state["df"]        = df
        st.session_state["anomalies"] = anomalies

df        = st.session_state["df"]
anomalies = st.session_state["anomalies"]

# ── KPIs ───────────────────────────────────────────────────────────────────
avg_score    = df["health_score"].mean()
min_score    = df["health_score"].min()
fault_count  = int(df["fault_injected"].sum())
critical_count = len([a for a in anomalies if a.severity == "critical"])
status_label = get_status_label(avg_score)
status_color = get_status_color(avg_score)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Avg Health Score", f"{avg_score:.1f}/100")
with col2:
    st.metric("Min Health Score", f"{min_score:.1f}/100")
with col3:
    st.metric("Total Readings", f"{len(df):,}")
with col4:
    st.metric("Fault Events", fault_count)
with col5:
    st.metric("Critical Anomalies", critical_count)

st.markdown(
    f"**Engine Status:** "
    f"<span style='color:{status_color}; font-weight:bold; "
    f"font-size:1.1em'>{status_label}</span>",
    unsafe_allow_html=True
)

st.divider()

# ── Health Score Timeline ──────────────────────────────────────────────────
st.subheader("Health Score Timeline")

fig_health = go.Figure()
fig_health.add_trace(go.Scatter(
    x=df.index,
    y=df["health_score"],
    mode="lines",
    fill="tozeroy",
    fillcolor="rgba(52, 152, 219, 0.1)",
    line=dict(color=COLORS["line"], width=1.5),
    name="Health Score",
    hovertemplate="<b>%{y:.1f}/100</b><br>%{x}<extra></extra>"
))

for threshold, color, label in [
    (90, COLORS["OPTIMAL"], "Optimal"),
    (75, COLORS["GOOD"],    "Good"),
    (60, COLORS["CAUTION"], "Caution"),
    (40, COLORS["ALERT"],   "Alert"),
]:
    fig_health.add_hline(
        y=threshold,
        line_dash="dash",
        line_color=color,
        line_width=1,
        annotation_text=label,
        annotation_position="right"
    )

fig_health.update_layout(
    height=280,
    template="plotly_dark",
    showlegend=False,
    margin=dict(l=40, r=80, t=20, b=40),
    yaxis=dict(range=[0, 105])
)
st.plotly_chart(fig_health, width="stretch")

# ── Engine parameters ──────────────────────────────────────────────────────
st.subheader("Engine Parameters")

params = [
    ("rpm",                 "RPM",                1, 1),
    ("temperature_exhaust", "Exhaust Temp (°C)",  1, 2),
    ("temperature_cooling", "Cooling Temp (°C)",  2, 1),
    ("pressure_lube",       "Lube Pressure (bar)", 2, 2),
    ("pressure_fuel",       "Fuel Pressure (bar)", 3, 1),
    ("vibration_rms",       "Vibration (mm/s)",    3, 2),
]

fig_params = make_subplots(
    rows=3, cols=2,
    subplot_titles=[p[1] for p in params],
    vertical_spacing=0.12,
    horizontal_spacing=0.08
)

# Dynamic y-axis ranges: columns with no natural fixed scale use
# [min, max*1.15] to leave 15% headroom above the observed peak.
param_yranges = {
    "rpm":                 (0,    df["rpm"].max() * 1.15)                 if "rpm" in df.columns else None,
    "temperature_exhaust": (50,   df["temperature_exhaust"].max() * 1.15) if "temperature_exhaust" in df.columns else None,
    "temperature_cooling": (30,   100),
    "pressure_lube":       (1.5,  6),
    "pressure_fuel":       (5,    11),
    "vibration_rms":       (0,    df["vibration_rms"].max() * 1.15)       if "vibration_rms" in df.columns else None,
}

for param, title, row, col in params:
    if param not in df.columns:
        continue
    fig_params.add_trace(
        go.Scatter(
            x=df.index,
            y=df[param],
            mode="lines",
            line=dict(color=COLORS["line"], width=1),
            name=title,
            hovertemplate=f"<b>{title}</b><br>%{{y:.2f}}<br>%{{x}}<extra></extra>"
        ),
        row=row, col=col
    )
    yrange = param_yranges.get(param)
    if yrange:
        fig_params.update_yaxes(range=list(yrange), row=row, col=col)

fig_params.update_layout(
    height=600,
    template="plotly_dark",
    showlegend=False,
    margin=dict(l=40, r=40, t=40, b=40)
)
st.plotly_chart(fig_params, width="stretch")

# ── Status distribution ────────────────────────────────────────────────────
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("Status Distribution")
    status_order  = ["OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"]
    status_counts = df["health_status"].value_counts()
    counts = [status_counts.get(s, 0) for s in status_order]
    colors = [COLORS[s] for s in status_order]
    total  = len(df)

    fig_dist = go.Figure(go.Bar(
        x=status_order,
        y=counts,
        marker_color=colors,
        text=[f"{c}<br>({c/total*100:.1f}%)" for c in counts],
        textposition="outside"
    ))
    fig_dist.update_layout(
        height=300,
        template="plotly_dark",
        showlegend=False,
        margin=dict(l=40, r=40, t=20, b=40)
    )
    st.plotly_chart(fig_dist, width="stretch")

with col_right:
    st.subheader("Recent Alerts")
    alert_df = df[
        df["health_status"].isin(["ALERT", "CRITICAL", "CAUTION"])
    ][["health_score", "health_status",
       "temperature_exhaust", "pressure_lube", "vibration_rms"]].tail(15).copy()

    alert_df = alert_df.rename(columns={
        "health_score":        "Health Score",
        "health_status":       "Status",
        "temperature_exhaust": "Exhaust Temp (°C)",
        "pressure_lube":       "Lube Pressure (bar)",
        "vibration_rms":       "Vibration (mm/s)",
    })

    if not alert_df.empty:
        st.dataframe(
            alert_df.style
                .format({
                    "Health Score":        "{:.2f}",
                    "Exhaust Temp (°C)":   "{:.2f}",
                    "Lube Pressure (bar)": "{:.2f}",
                    "Vibration (mm/s)":    "{:.2f}",
                })
                .background_gradient(
                    subset=["Health Score"],
                    cmap="RdYlGn",
                    vmin=0, vmax=100
                ),
            width="stretch",
            height=270
        )
    else:
        st.success("No alerts in current dataset")

# ── Download ───────────────────────────────────────────────────────────────
st.divider()
csv_data = df.to_csv().encode("utf-8")
st.download_button(
    label="⬇ Download processed data (CSV)",
    data=csv_data,
    file_name=f"engine_analysis_seed{seed}_{hours}h.csv",
    mime="text/csv"
)