import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="DR80 Tracking Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

.stApp {
    background: #0a0e1a;
    color: #e2e8f0;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #0d1221 !important;
    border-right: 1px solid #1e2d4a;
}
section[data-testid="stSidebar"] * {
    color: #94a3b8 !important;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stMultiSelect label {
    color: #64748b !important;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-family: 'IBM Plex Mono', monospace;
}

/* Metric cards */
.metric-card {
    background: linear-gradient(135deg, #111827 0%, #0f1729 100%);
    border: 1px solid #1e2d4a;
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 8px;
    position: relative;
    overflow: hidden;
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    background: #3b82f6;
}
.metric-card.green::before { background: #10b981; }
.metric-card.red::before { background: #ef4444; }
.metric-card.yellow::before { background: #f59e0b; }

.metric-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #475569;
    margin-bottom: 4px;
}
.metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.5rem;
    font-weight: 600;
    color: #e2e8f0;
}
.metric-sub {
    font-size: 0.75rem;
    color: #64748b;
    margin-top: 2px;
}

/* Section headers */
.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #3b82f6;
    border-bottom: 1px solid #1e2d4a;
    padding-bottom: 8px;
    margin-bottom: 16px;
    margin-top: 24px;
}

/* Positive/negative colors */
.pos { color: #10b981 !important; }
.neg { color: #ef4444 !important; }

/* Pill badge */
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 9999px;
    font-size: 0.7rem;
    font-family: 'IBM Plex Mono', monospace;
}
.badge-blue { background: rgba(59,130,246,0.15); color: #60a5fa; border: 1px solid rgba(59,130,246,0.3); }
.badge-green { background: rgba(16,185,129,0.15); color: #34d399; border: 1px solid rgba(16,185,129,0.3); }
.badge-red { background: rgba(239,68,68,0.15); color: #f87171; border: 1px solid rgba(239,68,68,0.3); }

/* Title */
.dashboard-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.4rem;
    font-weight: 600;
    color: #e2e8f0;
    letter-spacing: -0.02em;
}
.dashboard-sub {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    color: #334155;
    letter-spacing: 0.05em;
}

/* Plotly chart background fix */
.js-plotly-plot .plotly .bg { fill: transparent !important; }

/* Streamlit elements */
div[data-testid="stMetric"] {
    background: #111827;
    border: 1px solid #1e2d4a;
    border-radius: 8px;
    padding: 12px 16px;
}
div[data-testid="stMetric"] label { color: #64748b !important; font-size: 0.75rem; }
div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: #e2e8f0 !important; font-family: 'IBM Plex Mono', monospace; }

/* Dataframe */
.dataframe { font-size: 0.82rem; }

/* Multiselect tags */
.stMultiSelect span[data-baseweb="tag"] {
    background: rgba(59,130,246,0.2) !important;
    border: 1px solid rgba(59,130,246,0.4) !important;
    color: #93c5fd !important;
}

hr { border-color: #1e2d4a; }
</style>
""", unsafe_allow_html=True)


# ── Data Loading ─────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    df_raw = pd.read_excel("DR80_Tracking.xlsx", sheet_name="Current DR80", header=None)

    PERIODS = ["YTD", "1M", "3M", "6M", "1Y", "3Y", "5Y"]

    records = []
    current_sector = "Unknown"

    # Detect sector rows and data rows
    for i, row in df_raw.iterrows():
        col0 = row[0]
        col1 = str(row[1]) if pd.notna(row[1]) else ""
        col2 = str(row[2]) if pd.notna(row[2]) else ""
        col3 = str(row[3]) if pd.notna(row[3]) else ""

        # Sector header row: col0 is NaN, col1 is sector name, col2 is 'name'
        if pd.isna(col0) and col2 == "name" and col1 not in ["", "nan"]:
            current_sector = col1.strip()
            continue

        # Skip header/empty rows
        if col1 in ["", "nan"] or col2 in ["", "nan", "id", "name"]:
            continue
        if col1.startswith("Unnamed"):
            continue

        # Get ticker — col1 is ticker, col2 is name
        ticker = col1.strip()
        name = col2.strip()

        # Quarter (pipeline) field
        quarter = col3.strip() if col3 not in ["nan", ""] else None

        perf = {}
        for j, period in enumerate(PERIODS):
            val = row[4 + j]
            perf[period] = float(val) if pd.notna(val) else None

        # Is this a current DR80?
        is_current_dr = ticker.endswith("80 TB Equity")

        records.append({
            "Ticker": ticker,
            "Name": name,
            "Sector": current_sector,
            "Quarter": quarter,
            "Is_DR80": is_current_dr,
            **perf
        })

    return pd.DataFrame(records)


# ── Load & Filter Logic ───────────────────────────────────────────────────────
df = load_data()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:0.65rem;color:#334155;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:20px;">DR80 DASHBOARD v1.0</div>', unsafe_allow_html=True)

    st.markdown("**UNIVERSE**")
    universe_opt = st.radio(
        "",
        ["All", "Current DR80 Only", "Pipeline Only"],
        horizontal=False,
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**SECTORS**")
    all_sectors = sorted(df["Sector"].unique())
    selected_sectors = st.multiselect(
        "Filter sectors",
        options=all_sectors,
        default=all_sectors,
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**PIPELINE QUARTER**")
    pipeline_quarters = sorted([q for q in df["Quarter"].dropna().unique()])
    selected_quarters = st.multiselect(
        "Filter quarters",
        options=pipeline_quarters,
        default=pipeline_quarters,
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**PERFORMANCE PERIOD**")
    period = st.select_slider(
        "Return period",
        options=["YTD", "1M", "3M", "6M", "1Y", "3Y", "5Y"],
        value="YTD",
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**RETURN RANGE**")
    valid_vals = df[period].dropna()
    min_val, max_val = float(valid_vals.min()), float(valid_vals.max())
    ret_range = st.slider(
        "Range (%)",
        min_value=round(min_val, 0),
        max_value=round(max_val, 0),
        value=(round(min_val, 0), round(max_val, 0)),
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("**SEARCH**")
    search = st.text_input("Ticker or name", placeholder="e.g. NVDA, Apple...", label_visibility="collapsed")


# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()

if universe_opt == "Current DR80 Only":
    filtered = filtered[filtered["Is_DR80"]]
elif universe_opt == "Pipeline Only":
    filtered = filtered[~filtered["Is_DR80"]]

if selected_sectors:
    filtered = filtered[filtered["Sector"].isin(selected_sectors)]

# For pipeline quarter filter — only restrict non-DR80 rows
pipeline_mask = (filtered["Is_DR80"]) | (filtered["Quarter"].isin(selected_quarters))
filtered = filtered[pipeline_mask]

# Return range filter
filtered = filtered[
    (filtered[period].isna()) |
    ((filtered[period] >= ret_range[0]) & (filtered[period] <= ret_range[1]))
]

if search:
    s = search.lower()
    filtered = filtered[
        filtered["Ticker"].str.lower().str.contains(s, na=False) |
        filtered["Name"].str.lower().str.contains(s, na=False)
    ]


# ── Helper: color scale ────────────────────────────────────────────────────────
def color_val(v):
    if v is None or np.isnan(v): return "#475569"
    return "#10b981" if v >= 0 else "#ef4444"

def fmt_pct(v, decimals=1):
    if v is None or (isinstance(v, float) and np.isnan(v)): return "—"
    return f"+{v:.{decimals}f}%" if v >= 0 else f"{v:.{decimals}f}%"


# ── Header ─────────────────────────────────────────────────────────────────────
col_t, col_r = st.columns([3, 1])
with col_t:
    st.markdown('<div class="dashboard-title">DR80 TRACKING DASHBOARD</div>', unsafe_allow_html=True)
    st.markdown('<div class="dashboard-sub">KRUNGTHAI BANK SECURITIES — DEPOSITARY RECEIPT OPERATIONS</div>', unsafe_allow_html=True)
with col_r:
    st.markdown(f'<div style="text-align:right;font-family:IBM Plex Mono;font-size:0.65rem;color:#334155;margin-top:8px;">SHOWING {len(filtered)} / {len(df)} SECURITIES<br>PERIOD: <span style="color:#3b82f6">{period}</span></div>', unsafe_allow_html=True)

st.markdown("---")

# ── KPI Row ────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

period_vals = filtered[period].dropna()
positive_count = (period_vals >= 0).sum()
negative_count = (period_vals < 0).sum()
avg_ret = period_vals.mean() if len(period_vals) > 0 else 0
best = filtered.loc[filtered[period].idxmax()] if len(period_vals) > 0 else None
worst = filtered.loc[filtered[period].idxmin()] if len(period_vals) > 0 else None

with k1:
    st.markdown(f"""<div class="metric-card">
    <div class="metric-label">Total Securities</div>
    <div class="metric-value">{len(filtered)}</div>
    <div class="metric-sub">DR80: {filtered['Is_DR80'].sum()} | Pipeline: {(~filtered['Is_DR80']).sum()}</div>
    </div>""", unsafe_allow_html=True)

with k2:
    color_class = "green" if avg_ret >= 0 else "red"
    st.markdown(f"""<div class="metric-card {color_class}">
    <div class="metric-label">Avg Return ({period})</div>
    <div class="metric-value" style="color:{'#10b981' if avg_ret>=0 else '#ef4444'}">{fmt_pct(avg_ret)}</div>
    <div class="metric-sub">{positive_count} positive · {negative_count} negative</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""<div class="metric-card green">
    <div class="metric-label">Best Performer</div>
    <div class="metric-value" style="color:#10b981">{fmt_pct(best[period]) if best is not None else '—'}</div>
    <div class="metric-sub">{best['Ticker'].replace(' TB Equity','').replace(' US Equity','').replace(' HK Equity','').replace(' CH Equity','').replace(' JP Equity','') if best is not None else '—'}</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""<div class="metric-card red">
    <div class="metric-label">Worst Performer</div>
    <div class="metric-value" style="color:#ef4444">{fmt_pct(worst[period]) if worst is not None else '—'}</div>
    <div class="metric-sub">{worst['Ticker'].replace(' TB Equity','').replace(' US Equity','').replace(' HK Equity','').replace(' CH Equity','').replace(' JP Equity','') if worst is not None else '—'}</div>
    </div>""", unsafe_allow_html=True)

with k5:
    hit_rate = (positive_count / len(period_vals) * 100) if len(period_vals) > 0 else 0
    st.markdown(f"""<div class="metric-card">
    <div class="metric-label">Win Rate</div>
    <div class="metric-value">{hit_rate:.0f}%</div>
    <div class="metric-sub">Securities with positive {period} return</div>
    </div>""", unsafe_allow_html=True)

# ── Charts Row 1 ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">PERFORMANCE OVERVIEW</div>', unsafe_allow_html=True)

chart_cols = st.columns([3, 2])

with chart_cols[0]:
    # Horizontal bar chart - top & bottom performers
    plot_df = filtered[["Ticker", "Name", "Sector", "Is_DR80", period]].dropna(subset=[period]).copy()
    plot_df["ShortTicker"] = plot_df["Ticker"].str.replace(r"\s+(TB|US|HK|CH|JP)\s+Equity", "", regex=True)
    plot_df = plot_df.sort_values(period, ascending=False)

    n_show = min(30, len(plot_df))
    # Take top N/2 and bottom N/2
    half = n_show // 2
    top_df = plot_df.head(half)
    bot_df = plot_df.tail(half)
    bar_df = pd.concat([top_df, bot_df]).drop_duplicates().sort_values(period)

    fig_bar = go.Figure()
    colors = ["#10b981" if v >= 0 else "#ef4444" for v in bar_df[period]]
    fig_bar.add_trace(go.Bar(
        x=bar_df[period],
        y=bar_df["ShortTicker"],
        orientation='h',
        marker_color=colors,
        marker_line_width=0,
        hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<br>Return: %{x:.1f}%<extra></extra>",
        customdata=list(zip(bar_df["ShortTicker"], bar_df["Name"]))
    ))
    fig_bar.add_vline(x=0, line_color="#334155", line_width=1)
    fig_bar.update_layout(
        title=dict(text=f"Top & Bottom Performers — {period}", font=dict(family="IBM Plex Mono", size=11, color="#64748b"), x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono", color="#94a3b8", size=9),
        height=420,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis=dict(showgrid=True, gridcolor="#1e2d4a", zeroline=False, ticksuffix="%", title=""),
        yaxis=dict(showgrid=False, title=""),
        showlegend=False,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with chart_cols[1]:
    # Sector average returns donut/bar
    sector_perf = (
        filtered.groupby("Sector")[period]
        .mean()
        .dropna()
        .sort_values(ascending=True)
    )

    fig_sec = go.Figure()
    colors_sec = ["#10b981" if v >= 0 else "#ef4444" for v in sector_perf.values]
    fig_sec.add_trace(go.Bar(
        x=sector_perf.values,
        y=sector_perf.index,
        orientation="h",
        marker_color=colors_sec,
        marker_line_width=0,
        hovertemplate="<b>%{y}</b><br>Avg Return: %{x:.1f}%<extra></extra>",
        text=[f"{v:+.1f}%" for v in sector_perf.values],
        textposition="outside",
        textfont=dict(family="IBM Plex Mono", size=9, color="#94a3b8")
    ))
    fig_sec.add_vline(x=0, line_color="#334155", line_width=1)
    fig_sec.update_layout(
        title=dict(text=f"Avg Return by Sector — {period}", font=dict(family="IBM Plex Mono", size=11, color="#64748b"), x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono", color="#94a3b8", size=9),
        height=420,
        margin=dict(l=10, r=80, t=40, b=10),
        xaxis=dict(showgrid=True, gridcolor="#1e2d4a", zeroline=False, ticksuffix="%", title=""),
        yaxis=dict(showgrid=False, title=""),
        showlegend=False,
    )
    st.plotly_chart(fig_sec, use_container_width=True)


# ── Charts Row 2 ──────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">RETURN DISTRIBUTION & MULTI-PERIOD</div>', unsafe_allow_html=True)

c1, c2 = st.columns([2, 3])

with c1:
    # Distribution histogram
    hist_vals = filtered[period].dropna()
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Histogram(
        x=hist_vals,
        nbinsx=25,
        marker_color="#3b82f6",
        marker_line_color="#1e2d4a",
        marker_line_width=1,
        opacity=0.8,
        hovertemplate="Return: %{x:.1f}%<br>Count: %{y}<extra></extra>",
    ))
    fig_hist.add_vline(x=0, line_color="#ef4444", line_width=1, line_dash="dash")
    fig_hist.add_vline(x=hist_vals.mean(), line_color="#f59e0b", line_width=1, line_dash="dot",
                       annotation_text=f"avg {hist_vals.mean():+.1f}%", annotation_font_color="#f59e0b",
                       annotation_font_size=9, annotation_font_family="IBM Plex Mono")
    fig_hist.update_layout(
        title=dict(text=f"Return Distribution — {period}", font=dict(family="IBM Plex Mono", size=11, color="#64748b"), x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono", color="#94a3b8", size=9),
        height=300,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis=dict(showgrid=True, gridcolor="#1e2d4a", zeroline=False, ticksuffix="%", title=""),
        yaxis=dict(showgrid=True, gridcolor="#1e2d4a", title=""),
        bargap=0.05,
    )
    st.plotly_chart(fig_hist, use_container_width=True)

with c2:
    # Multi-period heatmap — top 20 by absolute YTD return
    PERIODS_ALL = ["YTD", "1M", "3M", "6M", "1Y", "3Y", "5Y"]
    heat_df = filtered[["Ticker", "Name"] + PERIODS_ALL].copy()
    heat_df["ShortTicker"] = heat_df["Ticker"].str.replace(r"\s+(TB|US|HK|CH|JP)\s+Equity", "", regex=True)
    heat_df["abs_ytd"] = heat_df["YTD"].abs()
    heat_df = heat_df.dropna(subset=["YTD"]).nlargest(20, "abs_ytd")

    z = heat_df[PERIODS_ALL].values
    y_labels = heat_df["ShortTicker"].tolist()

    fig_heat = go.Figure(data=go.Heatmap(
        z=z,
        x=PERIODS_ALL,
        y=y_labels,
        colorscale=[[0, "#7f1d1d"], [0.5, "#1e2d4a"], [1, "#065f46"]],
        zmid=0,
        hovertemplate="<b>%{y}</b> — %{x}<br>Return: %{z:.1f}%<extra></extra>",
        text=[[f"{v:+.0f}%" if not np.isnan(v) else "—" for v in row] for row in z],
        texttemplate="%{text}",
        textfont=dict(family="IBM Plex Mono", size=8),
        showscale=True,
        colorbar=dict(
            tickfont=dict(family="IBM Plex Mono", size=8, color="#64748b"),
            ticksuffix="%",
            thickness=10,
            len=0.8,
        )
    ))
    fig_heat.update_layout(
        title=dict(text="Multi-Period Return Heatmap (Top 20 by |YTD|)", font=dict(family="IBM Plex Mono", size=11, color="#64748b"), x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono", color="#94a3b8", size=9),
        height=300,
        margin=dict(l=10, r=60, t=40, b=10),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False, autorange="reversed"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)


# ── Pipeline Section ─────────────────────────────────────────────────────────
pipeline_df = filtered[~filtered["Is_DR80"]].copy()

if len(pipeline_df) > 0:
    st.markdown('<div class="section-header">PIPELINE — CANDIDATE SECURITIES</div>', unsafe_allow_html=True)

    p1, p2 = st.columns([1, 2])

    with p1:
        # Pipeline by quarter donut
        q_counts = pipeline_df["Quarter"].value_counts().sort_index()
        fig_pie = go.Figure(data=go.Pie(
            labels=q_counts.index,
            values=q_counts.values,
            hole=0.6,
            marker_colors=["#3b82f6", "#10b981", "#f59e0b", "#8b5cf6"],
            textfont=dict(family="IBM Plex Mono", size=10),
            hovertemplate="<b>%{label}</b><br>%{value} securities<extra></extra>",
        ))
        fig_pie.update_layout(
            title=dict(text="Pipeline by Quarter", font=dict(family="IBM Plex Mono", size=11, color="#64748b"), x=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=9),
            height=280,
            margin=dict(l=10, r=10, t=40, b=10),
            legend=dict(font=dict(family="IBM Plex Mono", size=9, color="#64748b")),
            annotations=[dict(text=f"<b>{len(pipeline_df)}</b><br>total", x=0.5, y=0.5, showarrow=False,
                              font=dict(family="IBM Plex Mono", size=12, color="#e2e8f0"))],
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with p2:
        # Pipeline scatter — YTD vs 1Y colored by sector
        scatter_df = pipeline_df.dropna(subset=["YTD", "1Y"]).copy()
        scatter_df["ShortTicker"] = scatter_df["Ticker"].str.replace(r"\s+(TB|US|HK|CH|JP)\s+Equity", "", regex=True)

        fig_scat = go.Figure()
        sector_colors = ["#3b82f6", "#10b981", "#f59e0b", "#8b5cf6", "#ef4444", "#06b6d4", "#f97316", "#84cc16"]
        for i, (sec, grp) in enumerate(scatter_df.groupby("Sector")):
            fig_scat.add_trace(go.Scatter(
                x=grp["YTD"],
                y=grp["1Y"],
                mode="markers+text",
                name=sec,
                marker=dict(color=sector_colors[i % len(sector_colors)], size=8, opacity=0.85),
                text=grp["ShortTicker"],
                textposition="top center",
                textfont=dict(family="IBM Plex Mono", size=8, color="#94a3b8"),
                hovertemplate="<b>%{text}</b><br>YTD: %{x:.1f}%<br>1Y: %{y:.1f}%<extra></extra>",
            ))

        fig_scat.add_hline(y=0, line_color="#334155", line_width=1)
        fig_scat.add_vline(x=0, line_color="#334155", line_width=1)
        fig_scat.update_layout(
            title=dict(text="Pipeline: YTD vs 1-Year Return", font=dict(family="IBM Plex Mono", size=11, color="#64748b"), x=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=9),
            height=280,
            margin=dict(l=10, r=10, t=40, b=10),
            xaxis=dict(showgrid=True, gridcolor="#1e2d4a", zeroline=False, ticksuffix="%", title="YTD Return"),
            yaxis=dict(showgrid=True, gridcolor="#1e2d4a", zeroline=False, ticksuffix="%", title="1-Year Return"),
            legend=dict(font=dict(family="IBM Plex Mono", size=8, color="#64748b"), orientation="h", y=-0.15),
        )
        st.plotly_chart(fig_scat, use_container_width=True)


# ── Data Table ────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">SECURITY TABLE</div>', unsafe_allow_html=True)

PERIODS_ALL = ["YTD", "1M", "3M", "6M", "1Y", "3Y", "5Y"]
table_df = filtered[["Ticker", "Name", "Sector", "Is_DR80", "Quarter"] + PERIODS_ALL].copy()
table_df["Ticker"] = table_df["Ticker"].str.replace(r"\s+(TB|US|HK|CH|JP)\s+Equity", "", regex=True)
table_df["Type"] = table_df["Is_DR80"].map({True: "DR80", False: "Pipeline"})
table_df = table_df.drop(columns=["Is_DR80"])
table_df = table_df.rename(columns={"Quarter": "Q"})

# Sort options
sort_col = st.selectbox("Sort by", ["Name", "Sector", "YTD", "1M", "3M", "6M", "1Y"], index=2, label_visibility="collapsed")
sort_asc = st.checkbox("Ascending", value=False)
table_df = table_df.sort_values(sort_col, ascending=sort_asc, na_position="last")

# Style the dataframe
def style_pct(val):
    if pd.isna(val): return "color: #475569"
    return "color: #10b981" if val >= 0 else "color: #ef4444"

styled = table_df.style.applymap(
    style_pct,
    subset=PERIODS_ALL
).format(
    {p: lambda x: f"{x:+.1f}%" if pd.notna(x) else "—" for p in PERIODS_ALL}
).set_properties(**{
    "font-family": "IBM Plex Mono",
    "font-size": "12px",
    "background-color": "#0a0e1a",
    "color": "#94a3b8",
    "border-color": "#1e2d4a",
})

st.dataframe(styled, use_container_width=True, height=450)

# ── Download ──────────────────────────────────────────────────────────────────
csv = table_df.to_csv(index=False)
st.download_button(
    "⬇ Export to CSV",
    data=csv,
    file_name=f"DR80_filtered_{period}.csv",
    mime="text/csv",
    help="Download the currently filtered data"
)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<div style="font-family:IBM Plex Mono;font-size:0.6rem;color:#1e2d4a;text-align:center;">KTB SECURITIES · DEPOSITARY RECEIPT OPERATIONS · DR80 TRACKING SYSTEM</div>',
    unsafe_allow_html=True
)
