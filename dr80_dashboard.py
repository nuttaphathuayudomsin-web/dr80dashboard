"""
DR80 Tracking Dashboard
KTB Securities — Depositary Receipt Operations
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import io, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="DR80 Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background: #0a0e1a; color: #e2e8f0; }
section[data-testid="stSidebar"] { background: #0d1221 !important; border-right: 1px solid #1e2d4a; }
section[data-testid="stSidebar"] * { color: #94a3b8 !important; }
.metric-card { background: linear-gradient(135deg,#111827,#0f1729); border:1px solid #1e2d4a; border-radius:8px; padding:16px 20px; margin-bottom:8px; position:relative; overflow:hidden; }
.metric-card::before { content:''; position:absolute; top:0; left:0; width:3px; height:100%; background:#3b82f6; }
.metric-card.green::before { background:#10b981; }
.metric-card.red::before { background:#ef4444; }
.metric-label { font-family:'IBM Plex Mono',monospace; font-size:0.65rem; text-transform:uppercase; letter-spacing:0.1em; color:#475569; margin-bottom:4px; }
.metric-value { font-family:'IBM Plex Mono',monospace; font-size:1.5rem; font-weight:600; color:#e2e8f0; }
.metric-sub { font-size:0.75rem; color:#64748b; margin-top:2px; }
.section-header { font-family:'IBM Plex Mono',monospace; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.12em; color:#3b82f6; border-bottom:1px solid #1e2d4a; padding-bottom:8px; margin-bottom:16px; margin-top:24px; }
.dashboard-title { font-family:'IBM Plex Mono',monospace; font-size:1.4rem; font-weight:600; color:#e2e8f0; letter-spacing:-0.02em; }
.dashboard-sub { font-family:'IBM Plex Mono',monospace; font-size:0.7rem; color:#334155; letter-spacing:0.05em; }
.stMultiSelect span[data-baseweb="tag"] { background:rgba(59,130,246,0.2)!important; border:1px solid rgba(59,130,246,0.4)!important; color:#93c5fd!important; }
hr { border-color:#1e2d4a; }
.stButton > button { background:#1e2d4a; color:#94a3b8; border:1px solid #2d3f5e; border-radius:6px; font-family:'IBM Plex Mono',monospace; font-size:0.75rem; }
.stButton > button:hover { background:#2d3f5e; color:#e2e8f0; border-color:#3b82f6; }
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
PERIODS = ["YTD", "1M", "3M", "6M", "1Y", "3Y", "5Y"]
SECTORS = [
    "Semiconductor/ AI", "Techonology", "Precious Metal",
    "Energy", "Consumer discretionary", "Consumer defensive",
    "Defense", "ETF"
]
DEFAULT_FILE = "DR80_Tracking.xlsx"


# ── Ticker conversion ──────────────────────────────────────────────────────────
def bbg_to_yahoo(bbg: str):
    t = bbg.strip()
    parts = t.rsplit(" ", 2)
    if len(parts) < 3 or parts[2].upper() != "EQUITY":
        return t or None
    code, exch = parts[0].strip(), parts[1].strip().upper()
    if exch == "TB":
        return None  # Thai DRs not on Yahoo
    if exch == "US":
        return code
    if exch == "HK":
        try:
            return f"{int(code):04d}.HK"
        except ValueError:
            return f"{code}.HK"
    if exch == "JP":
        return f"{code}.T"
    if exch == "CH":
        try:
            int(code)
            return f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
        except ValueError:
            return f"{code}.SS"
    mapping = {"SP": ".SI", "LN": ".L", "FP": ".PA", "GR": ".DE", "AU": ".AX"}
    suffix = mapping.get(exch, "")
    return f"{code}{suffix}" if suffix else code


def short_ticker(bbg: str) -> str:
    """Strip exchange suffix, return raw code."""
    for suffix in [" TB Equity", " US Equity", " HK Equity", " JP Equity",
                   " CH Equity", " SP Equity", " LN Equity", " FP Equity",
                   " GR Equity", " AU Equity"]:
        bbg = bbg.replace(suffix, "")
    return bbg.strip()


def display_label(bbg: str, name: str, max_len: int = 15) -> str:
    """
    For numeric-code tickers (e.g. 300476, 9984), return a shortened company name.
    For alpha tickers (AAPL, NVDA80), return the stripped ticker code.
    Always returns a plain STRING — never a number — so Plotly treats it as a category.
    """
    code = short_ticker(bbg)
    # If the code is purely numeric, use shortened company name instead
    try:
        int(code)
        # It's a number — use company name, trimmed
        label = name.strip()
        if len(label) > max_len:
            label = label[:max_len].rstrip() + "…"
        return label
    except ValueError:
        # Alpha ticker — strip the "80" suffix for DR80 tickers to keep it short
        return code


# ── Excel parsing ──────────────────────────────────────────────────────────────
def parse_excel(file_obj) -> pd.DataFrame:
    df_raw = pd.read_excel(file_obj, sheet_name="Current DR80", header=None)
    records = []
    current_sector = "Unknown"
    for _, row in df_raw.iterrows():
        c0 = row[0]
        c1 = str(row[1]) if pd.notna(row[1]) else ""
        c2 = str(row[2]) if pd.notna(row[2]) else ""
        c3 = str(row[3]) if pd.notna(row[3]) else ""
        if pd.isna(c0) and c2 == "name" and c1 not in ["", "nan"]:
            current_sector = c1.strip()
            continue
        if c1 in ["", "nan"] or c2 in ["", "nan", "id", "name"] or c1.startswith("Unnamed"):
            continue
        perf = {}
        for j, p in enumerate(PERIODS):
            v = row[4 + j] if (4 + j) < len(row) else None
            perf[p] = float(v) if pd.notna(v) else None
        records.append({
            "BBG_Ticker": c1.strip(),
            "Yahoo_Ticker": bbg_to_yahoo(c1.strip()),
            "Name": c2.strip(),
            "Sector": current_sector,
            "Quarter": c3.strip() if c3 not in ["nan", ""] else None,
            "Is_DR80": c1.strip().endswith("80 TB Equity"),
            **perf,
        })
    return pd.DataFrame(records)


# ── Yahoo Finance fetch ────────────────────────────────────────────────────────
def fetch_returns_yahoo(tickers: list) -> dict:
    try:
        import yfinance as yf
    except ImportError:
        st.error("yfinance not installed. Run: pip install yfinance")
        return {}

    today = datetime.today()
    start = (today - timedelta(days=365 * 5 + 30)).strftime("%Y-%m-%d")
    valid = [t for t in tickers if t]
    if not valid:
        return {}

    progress = st.progress(0, text="Connecting to Yahoo Finance...")
    try:
        raw = yf.download(valid, start=start, end=today.strftime("%Y-%m-%d"),
                          auto_adjust=True, progress=False)
        prices = raw["Close"] if "Close" in raw.columns else raw
    except Exception as e:
        st.error(f"Download failed: {e}")
        progress.empty()
        return {}

    def pct_chg(series, from_dt):
        s = series.dropna()
        idx = s.index.searchsorted(pd.Timestamp(from_dt))
        if idx >= len(s):
            return None
        sp, ep = s.iloc[idx], s.iloc[-1]
        return (ep / sp - 1) * 100 if sp != 0 else None

    period_offsets = {
        "YTD": datetime(today.year, 1, 1),
        "1M":  today - timedelta(days=30),
        "3M":  today - timedelta(days=91),
        "6M":  today - timedelta(days=182),
        "1Y":  today - timedelta(days=365),
        "3Y":  today - timedelta(days=365 * 3),
        "5Y":  today - timedelta(days=365 * 5),
    }

    results = {}
    for i, ticker in enumerate(valid):
        progress.progress((i + 1) / len(valid), text=f"Processing {ticker}…")
        try:
            s = prices[ticker] if (isinstance(prices, pd.DataFrame) and ticker in prices.columns) else prices
            results[ticker] = {p: pct_chg(s, dt) for p, dt in period_offsets.items()}
        except Exception:
            results[ticker] = {p: None for p in PERIODS}

    progress.empty()
    return results


def fetch_single(yahoo_ticker: str) -> dict:
    """Fetch return data for a single ticker."""
    res = fetch_returns_yahoo([yahoo_ticker])
    return res.get(yahoo_ticker, {p: None for p in PERIODS})


# ── Write back Excel ───────────────────────────────────────────────────────────
def write_excel(original_bytes: bytes, df: pd.DataFrame) -> bytes:
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(original_bytes))
    ws = wb["Current DR80"]

    raw = pd.read_excel(io.BytesIO(original_bytes), sheet_name="Current DR80", header=None)
    df_idx = df.set_index("BBG_Ticker")
    sector_last = {}
    current_sector = None
    original_bbg = set()

    for i, row in raw.iterrows():
        c1 = str(row[1]) if pd.notna(row[1]) else ""
        c2 = str(row[2]) if pd.notna(row[2]) else ""
        if c2 == "name" and c1 not in ["", "nan"] and pd.isna(row[0]):
            current_sector = c1.strip()
            continue
        if c1 in ["", "nan"] or c1.startswith("Unnamed"):
            continue
        bbg = c1.strip()
        original_bbg.add(bbg)
        if current_sector:
            sector_last[current_sector] = i + 1  # 1-indexed
        # Update return cells
        if bbg in df_idx.index:
            rec = df_idx.loc[bbg]
            for j, p in enumerate(PERIODS):
                v = rec[p]
                ws.cell(row=i + 1, column=5 + j).value = round(float(v), 6) if pd.notna(v) and v is not None else None

    # Append new pipeline rows
    new_rows = df[~df["BBG_Ticker"].isin(original_bbg) & ~df["Is_DR80"]]
    for _, rec in new_rows.iterrows():
        sector = rec["Sector"]
        insert_after = sector_last.get(sector, ws.max_row)
        ws.insert_rows(insert_after + 1)
        nr = insert_after + 1
        code = rec["BBG_Ticker"].rsplit(" ", 2)[0].strip()
        ws.cell(row=nr, column=1).value = code
        ws.cell(row=nr, column=2).value = rec["BBG_Ticker"]
        ws.cell(row=nr, column=3).value = rec["Name"]
        ws.cell(row=nr, column=4).value = rec["Quarter"]
        for j, p in enumerate(PERIODS):
            v = rec[p]
            ws.cell(row=nr, column=5 + j).value = round(float(v), 6) if pd.notna(v) and v is not None else None
        sector_last[sector] = nr  # stack subsequent inserts

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ── Chart helpers ──────────────────────────────────────────────────────────────
C = {"bg": "rgba(0,0,0,0)", "grid": "#1e2d4a", "text": "#94a3b8",
     "pos": "#10b981", "neg": "#ef4444", "blue": "#3b82f6", "font": "IBM Plex Mono"}

def base_layout(h=350, margin=None):
    m = margin or dict(l=10, r=10, t=44, b=10)
    return dict(paper_bgcolor=C["bg"], plot_bgcolor=C["bg"],
                font=dict(family=C["font"], color=C["text"], size=11),
                height=h, margin=m)

def fmt_pct(v, d=1):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"+{v:.{d}f}%" if v >= 0 else f"{v:.{d}f}%"

def style_pct(v):
    if pd.isna(v): return "color:#475569"
    return f"color:{C['pos']}" if v >= 0 else f"color:{C['neg']}"

def bar_colors(vals):
    return [C["pos"] if v >= 0 else C["neg"] for v in vals]


# ── Session state ──────────────────────────────────────────────────────────────
for key, default in [("df", None), ("excel_bytes", None),
                     ("last_refresh", None), ("source_label", None)]:
    if key not in st.session_state:
        st.session_state[key] = default

# Auto-load default file
if st.session_state.df is None and os.path.exists(DEFAULT_FILE):
    with open(DEFAULT_FILE, "rb") as f:
        b = f.read()
    st.session_state.excel_bytes = b
    st.session_state.df = parse_excel(io.BytesIO(b))
    st.session_state.source_label = DEFAULT_FILE


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown('<div style="font-family:IBM Plex Mono;font-size:0.6rem;color:#334155;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:16px;">DR80 DASHBOARD · KTB Securities</div>', unsafe_allow_html=True)

    # Upload
    st.markdown("**📂 DATA SOURCE**")
    uploaded = st.file_uploader("Upload Excel", type=["xlsx"], label_visibility="collapsed")
    if uploaded:
        b = uploaded.read()
        st.session_state.excel_bytes = b
        st.session_state.df = parse_excel(io.BytesIO(b))
        st.session_state.source_label = uploaded.name
        st.session_state.last_refresh = None
        st.success(f"✓ Loaded {uploaded.name}")

    if st.session_state.source_label:
        st.caption(f"Source: {st.session_state.source_label}")
    if st.session_state.last_refresh:
        st.caption(f"Last refresh: {st.session_state.last_refresh}")

    st.markdown("---")

    # Refresh
    st.markdown("**🔄 REFRESH DATA**")
    st.caption("Fetches live returns from Yahoo Finance for all non-TB securities.")
    if st.button("⟳ Refresh from Yahoo Finance", use_container_width=True):
        if st.session_state.df is None:
            st.error("Load a file first.")
        else:
            df_curr = st.session_state.df.copy()
            fetchable = df_curr[df_curr["Yahoo_Ticker"].notna()][["BBG_Ticker", "Yahoo_Ticker"]].drop_duplicates()
            fetched = fetch_returns_yahoo(fetchable["Yahoo_Ticker"].tolist())
            ymap = dict(zip(fetchable["Yahoo_Ticker"], fetchable["BBG_Ticker"]))
            n = 0
            for yticker, rets in fetched.items():
                bbg = ymap.get(yticker)
                if bbg is None: continue
                mask = df_curr["BBG_Ticker"] == bbg
                for p in PERIODS:
                    if rets.get(p) is not None:
                        df_curr.loc[mask, p] = rets[p]
                n += 1
            st.session_state.df = df_curr
            st.session_state.last_refresh = datetime.now().strftime("%Y-%m-%d %H:%M")
            st.success(f"✓ Updated {n} securities")

    st.markdown("---")

    # Filters (only if data loaded)
    if st.session_state.df is not None:
        df_all = st.session_state.df

        st.markdown("**FILTERS**")
        universe = st.radio("Universe", ["All", "DR80 Only", "Pipeline Only"], label_visibility="collapsed")

        all_sectors = sorted(df_all["Sector"].unique())
        sel_sectors = st.multiselect("Sectors", all_sectors, default=all_sectors, label_visibility="collapsed",
                                     placeholder="All sectors")

        all_qtrs = sorted([q for q in df_all["Quarter"].dropna().unique()])
        sel_qtrs = st.multiselect("Pipeline quarters", all_qtrs, default=all_qtrs, label_visibility="collapsed",
                                  placeholder="All quarters")

        period = st.select_slider("Period", PERIODS, value="YTD", label_visibility="collapsed")

        valid_p = df_all[period].dropna()
        if len(valid_p):
            mn, mx = float(valid_p.min()), float(valid_p.max())
            ret_range = st.slider("Return range (%)", min_value=round(mn), max_value=round(mx),
                                  value=(round(mn), round(mx)), label_visibility="collapsed")
        else:
            ret_range = (-500, 1000)

        search = st.text_input("🔍 Search", placeholder="Ticker or name…", label_visibility="collapsed")


# ── No data ────────────────────────────────────────────────────────────────────
if st.session_state.df is None:
    st.markdown('<div class="dashboard-title">DR80 TRACKING DASHBOARD</div>', unsafe_allow_html=True)
    st.info("Upload a `DR80_Tracking.xlsx` file in the sidebar to get started.")
    st.stop()


# ── Apply filters ──────────────────────────────────────────────────────────────
df_all = st.session_state.df.copy()
filt = df_all.copy()

if universe == "DR80 Only":
    filt = filt[filt["Is_DR80"]]
elif universe == "Pipeline Only":
    filt = filt[~filt["Is_DR80"]]

if sel_sectors:
    filt = filt[filt["Sector"].isin(sel_sectors)]

# Quarter filter only applies to pipeline rows
filt = filt[filt["Is_DR80"] | filt["Quarter"].isin(sel_qtrs)]

filt = filt[filt[period].isna() | ((filt[period] >= ret_range[0]) & (filt[period] <= ret_range[1]))]

if search:
    s = search.lower()
    filt = filt[filt["BBG_Ticker"].str.lower().str.contains(s, na=False) |
                filt["Name"].str.lower().str.contains(s, na=False)]


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_dash, tab_pipeline, tab_add = st.tabs(["📊  Dashboard", "🔭  Pipeline", "➕  Add Security"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_dash:
    ct, ci = st.columns([3, 1])
    with ct:
        st.markdown('<div class="dashboard-title">DR80 TRACKING</div>', unsafe_allow_html=True)
        st.markdown('<div class="dashboard-sub">KTB SECURITIES · DEPOSITARY RECEIPT OPERATIONS</div>', unsafe_allow_html=True)
    with ci:
        st.markdown(f'<div style="text-align:right;font-family:IBM Plex Mono;font-size:0.65rem;color:#334155;margin-top:8px;">SHOWING {len(filt)} / {len(df_all)}<br>PERIOD: <span style="color:#3b82f6">{period}</span></div>', unsafe_allow_html=True)

    st.markdown("---")

    # KPIs
    pv = filt[period].dropna()
    pos_n = int((pv >= 0).sum())
    neg_n = int((pv < 0).sum())
    avg_r = float(pv.mean()) if len(pv) else 0.0
    hit_r = pos_n / len(pv) * 100 if len(pv) else 0.0
    best = filt.loc[filt[period].idxmax()] if len(pv) else None
    worst = filt.loc[filt[period].idxmin()] if len(pv) else None

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Securities</div>
        <div class="metric-value">{len(filt)}</div>
        <div class="metric-sub">DR80: {filt['Is_DR80'].sum()} · Pipeline: {(~filt['Is_DR80']).sum()}</div></div>""", unsafe_allow_html=True)
    with k2:
        cc = "green" if avg_r >= 0 else "red"
        vc = C["pos"] if avg_r >= 0 else C["neg"]
        st.markdown(f"""<div class="metric-card {cc}"><div class="metric-label">Avg Return ({period})</div>
        <div class="metric-value" style="color:{vc}">{fmt_pct(avg_r)}</div>
        <div class="metric-sub">{pos_n} pos · {neg_n} neg</div></div>""", unsafe_allow_html=True)
    with k3:
        bv = best[period] if best is not None else None
        bt = display_label(best["BBG_Ticker"], best["Name"]) if best is not None else "—"
        st.markdown(f"""<div class="metric-card green"><div class="metric-label">Best ({period})</div>
        <div class="metric-value" style="color:#10b981">{fmt_pct(bv)}</div>
        <div class="metric-sub">{bt}</div></div>""", unsafe_allow_html=True)
    with k4:
        wv = worst[period] if worst is not None else None
        wt = display_label(worst["BBG_Ticker"], worst["Name"]) if worst is not None else "—"
        st.markdown(f"""<div class="metric-card red"><div class="metric-label">Worst ({period})</div>
        <div class="metric-value" style="color:#ef4444">{fmt_pct(wv)}</div>
        <div class="metric-sub">{wt}</div></div>""", unsafe_allow_html=True)
    with k5:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Win Rate</div>
        <div class="metric-value">{hit_r:.0f}%</div>
        <div class="metric-sub">+ve return for {period}</div></div>""", unsafe_allow_html=True)

    # Charts row 1
    st.markdown('<div class="section-header">PERFORMANCE</div>', unsafe_allow_html=True)
    ca, cb = st.columns([3, 2])

    with ca:
        pdf = filt[["BBG_Ticker", "Name", period]].dropna(subset=[period]).copy()
        pdf["S"] = pdf.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1)
        # Deduplicate labels by appending sector if collision
        seen = {}
        deduped = []
        for lbl in pdf["S"]:
            if lbl in seen:
                seen[lbl] += 1
                deduped.append(f"{lbl} ({seen[lbl]})")
            else:
                seen[lbl] = 0
                deduped.append(lbl)
        pdf["S"] = deduped
        pdf["S"] = pdf["S"].astype(str)  # force string — prevents Plotly treating numeric codes as numbers
        pdf = pdf.sort_values(period, ascending=False)
        half = min(15, len(pdf) // 2)
        bar_df = pd.concat([pdf.head(half), pdf.tail(half)]).drop_duplicates().sort_values(period)
        n_bars = len(bar_df)
        bar_h = max(320, n_bars * 28 + 60)
        fig = go.Figure(go.Bar(
            x=bar_df[period],
            y=bar_df["S"].astype(str),
            orientation="h",
            marker_color=bar_colors(bar_df[period]), marker_line_width=0,
            text=[f"{v:+.1f}%" for v in bar_df[period]],
            textposition="outside",
            textfont=dict(family=C["font"], size=10, color=C["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
        ))
        fig.add_vline(x=0, line_color="#334155", line_width=1)
        fig.update_layout(
            title=dict(text=f"Top & Bottom Performers — {period}", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
            **base_layout(bar_h),
            xaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%",
                       tickfont=dict(size=11)),
            yaxis=dict(showgrid=False, tickfont=dict(size=11), type="category"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with cb:
        sp = filt.groupby("Sector")[period].mean().dropna().sort_values()
        fig2 = go.Figure(go.Bar(
            x=sp.values, y=sp.index, orientation="h",
            marker_color=bar_colors(sp.values), marker_line_width=0,
            text=[f"{v:+.1f}%" for v in sp.values], textposition="outside",
            textfont=dict(family=C["font"], size=11, color=C["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
        ))
        fig2.add_vline(x=0, line_color="#334155", line_width=1)
        fig2.update_layout(title=dict(text=f"Avg by Sector — {period}", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
                           **base_layout(420, margin=dict(l=10, r=80, t=44, b=10)),
                           xaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%",
                                      tickfont=dict(size=11)),
                           yaxis=dict(showgrid=False, tickfont=dict(size=11)))
        st.plotly_chart(fig2, use_container_width=True)

    # Charts row 2
    st.markdown('<div class="section-header">DISTRIBUTION & HEATMAPS</div>', unsafe_allow_html=True)
    cc1, cc2 = st.columns([2, 3])

    with cc1:
        hv = filt[period].dropna()
        fig3 = go.Figure(go.Histogram(x=hv, nbinsx=20, marker_color=C["blue"],
                                      marker_line_color=C["grid"], marker_line_width=1, opacity=0.85))
        fig3.add_vline(x=0, line_color=C["neg"], line_width=1, line_dash="dash")
        if len(hv):
            fig3.add_vline(x=float(hv.mean()), line_color="#f59e0b", line_width=1, line_dash="dot",
                           annotation_text=f"avg {hv.mean():+.1f}%",
                           annotation_font=dict(color="#f59e0b", size=10, family=C["font"]))
        fig3.update_layout(title=dict(text=f"Distribution — {period}", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
                           **base_layout(340), xaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%",
                                                           tickfont=dict(size=11)),
                           yaxis=dict(showgrid=True, gridcolor=C["grid"], tickfont=dict(size=11)))
        st.plotly_chart(fig3, use_container_width=True)

    with cc2:
        # Security-level heatmap — bigger, better font, clearer colorbar
        hdf = filt[["BBG_Ticker", "Name"] + PERIODS].copy()
        hdf["S"] = hdf.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
        hdf["abs_ytd"] = hdf["YTD"].abs()
        hdf = hdf.dropna(subset=["YTD"]).nlargest(20, "abs_ytd")
        z = hdf[PERIODS].values
        n_rows = len(hdf)
        heat_h = max(380, n_rows * 26 + 60)

        # Build text with proper nan handling
        text_vals = []
        for row_z in z:
            row_t = []
            for v in row_z:
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    row_t.append("—")
                else:
                    row_t.append(f"{v:+.0f}%")
            text_vals.append(row_t)

        fig4 = go.Figure(go.Heatmap(
            z=z, x=PERIODS, y=hdf["S"].tolist(),
            colorscale=[
                [0.0, "#991b1b"],   # deep red  (very negative)
                [0.3, "#7f1d1d"],
                [0.5, "#0f172a"],   # near-black (zero)
                [0.7, "#064e3b"],
                [1.0, "#059669"],   # bright green (very positive)
            ],
            zmid=0,
            text=text_vals,
            texttemplate="%{text}",
            textfont=dict(family=C["font"], size=10, color="#e2e8f0"),
            hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
            colorbar=dict(
                title=dict(text="Return %", font=dict(family=C["font"], size=10, color="#64748b")),
                tickfont=dict(family=C["font"], size=10, color="#94a3b8"),
                ticksuffix="%",
                thickness=14,
                len=0.9,
                tickvals=[-100, -50, 0, 50, 100],
            ),
        ))
        fig4.update_layout(
            title=dict(text="Multi-Period Heatmap — Top 20 by |YTD|", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
            **base_layout(heat_h, margin=dict(l=10, r=80, t=44, b=10)),
            xaxis=dict(showgrid=False, tickfont=dict(size=12, color="#94a3b8"), side="bottom"),
            yaxis=dict(showgrid=False, autorange="reversed", tickfont=dict(size=11, color="#e2e8f0")),
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ── Sector Heatmap ─────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">SECTOR HEATMAP — AVG & MEDIAN RETURNS</div>', unsafe_allow_html=True)

    sector_rows_avg = []
    sector_rows_med = []
    sector_labels = []

    for sec in sorted(filt["Sector"].unique()):
        sec_data = filt[filt["Sector"] == sec]
        avgs = [sec_data[p].mean() if sec_data[p].notna().sum() > 0 else np.nan for p in PERIODS]
        meds = [sec_data[p].median() if sec_data[p].notna().sum() > 0 else np.nan for p in PERIODS]
        sector_rows_avg.append(avgs)
        sector_rows_med.append(meds)
        sector_labels.append(sec)

    # Interleave: for each sector, show avg row then median row
    z_sec = []
    y_sec = []
    for i, sec in enumerate(sector_labels):
        z_sec.append(sector_rows_avg[i])
        y_sec.append(f"{sec}  avg")
        z_sec.append(sector_rows_med[i])
        y_sec.append(f"{sec}  med")

    z_sec_arr = np.array(z_sec, dtype=float)
    sec_h = max(300, len(y_sec) * 22 + 60)

    text_sec = []
    for row_z in z_sec_arr:
        row_t = []
        for v in row_z:
            if np.isnan(v):
                row_t.append("—")
            else:
                row_t.append(f"{v:+.0f}%")
        text_sec.append(row_t)

    fig_sec_heat = go.Figure(go.Heatmap(
        z=z_sec_arr, x=PERIODS, y=y_sec,
        colorscale=[
            [0.0, "#991b1b"],
            [0.3, "#7f1d1d"],
            [0.5, "#0f172a"],
            [0.7, "#064e3b"],
            [1.0, "#059669"],
        ],
        zmid=0,
        text=text_sec,
        texttemplate="%{text}",
        textfont=dict(family=C["font"], size=11, color="#e2e8f0"),
        hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
        colorbar=dict(
            title=dict(text="Return %", font=dict(family=C["font"], size=10, color="#64748b")),
            tickfont=dict(family=C["font"], size=10, color="#94a3b8"),
            ticksuffix="%", thickness=14, len=0.9,
            tickvals=[-50, -25, 0, 25, 50],
        ),
    ))
    fig_sec_heat.update_layout(
        title=dict(text="Sector Returns by Period (Avg & Median)", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
        **base_layout(sec_h, margin=dict(l=10, r=80, t=44, b=10)),
        xaxis=dict(showgrid=False, tickfont=dict(size=12, color="#94a3b8")),
        yaxis=dict(showgrid=False, autorange="reversed", tickfont=dict(size=11, color="#e2e8f0")),
    )
    st.plotly_chart(fig_sec_heat, use_container_width=True)

    # Table
    st.markdown('<div class="section-header">SECURITY TABLE</div>', unsafe_allow_html=True)
    sc1, sc2 = st.columns([2, 1])
    with sc1:
        sort_by = st.selectbox("Sort by", ["Name", "Sector"] + PERIODS, index=2, label_visibility="collapsed")
    with sc2:
        asc = st.checkbox("Ascending", value=False)

    tbl = filt[["BBG_Ticker", "Name", "Sector", "Is_DR80", "Quarter"] + PERIODS].copy()
    tbl["Ticker"] = tbl.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
    tbl["Type"] = tbl["Is_DR80"].map({True: "DR80", False: "Pipeline"})
    tbl = tbl.drop(columns=["BBG_Ticker", "Is_DR80"]).rename(columns={"Quarter": "Q"})
    tbl = tbl[["Ticker", "Name", "Sector", "Type", "Q"] + PERIODS]
    tbl = tbl.sort_values(sort_by, ascending=asc, na_position="last")

    styled = (tbl.style
              .applymap(style_pct, subset=PERIODS)
              .format({p: lambda x: fmt_pct(x) for p in PERIODS})
              .set_properties(**{"font-family": "IBM Plex Mono", "font-size": "12px"}))
    st.dataframe(styled, use_container_width=True, height=430)

    # Export row
    e1, e2 = st.columns(2)
    with e1:
        st.download_button("⬇ Export CSV", data=tbl.to_csv(index=False),
                           file_name=f"DR80_{period}_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)
    with e2:
        if st.session_state.excel_bytes:
            xl_out = write_excel(st.session_state.excel_bytes, st.session_state.df)
            st.download_button("⬇ Export Updated Excel", data=xl_out,
                               file_name=f"DR80_Tracking_{datetime.now().strftime('%Y%m%d')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    pipe_df_all = df_all[~df_all["Is_DR80"]].copy()

    st.markdown('<div class="section-header">PIPELINE OVERVIEW</div>', unsafe_allow_html=True)

    if len(pipe_df_all) == 0:
        st.info("No pipeline securities found.")
    else:
        # Sector filter for pipeline tab
        pipe_sectors = sorted(pipe_df_all["Sector"].unique())
        pipe_sel_sectors = st.multiselect(
            "Filter by sector",
            options=pipe_sectors,
            default=pipe_sectors,
            label_visibility="collapsed",
            placeholder="All sectors",
            key="pipe_sector_filter"
        )
        pipe_df = pipe_df_all[pipe_df_all["Sector"].isin(pipe_sel_sectors)] if pipe_sel_sectors else pipe_df_all

        st.caption(f"Showing {len(pipe_df)} of {len(pipe_df_all)} pipeline securities")
        p1, p2, p3 = st.columns(3)

        with p1:
            qc = pipe_df["Quarter"].value_counts().sort_index()
            fp = go.Figure(go.Pie(labels=qc.index, values=qc.values, hole=0.6,
                                  marker_colors=["#3b82f6","#10b981","#f59e0b","#8b5cf6"],
                                  textfont=dict(family=C["font"], size=12),
                                  hovertemplate="<b>%{label}</b><br>%{value}<extra></extra>"))
            fp.update_layout(title=dict(text="By Quarter", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
                             **base_layout(280), legend=dict(font=dict(family=C["font"], size=11, color="#64748b")),
                             annotations=[dict(text=f"<b>{len(pipe_df)}</b><br>total", x=0.5, y=0.5,
                                               showarrow=False, font=dict(family=C["font"], size=13, color="#e2e8f0"))])
            st.plotly_chart(fp, use_container_width=True)

        with p2:
            sc = pipe_df["Sector"].value_counts()
            fs = go.Figure(go.Pie(labels=sc.index, values=sc.values, hole=0.5,
                                  textfont=dict(family=C["font"], size=11),
                                  hovertemplate="<b>%{label}</b><br>%{value}<extra></extra>"))
            fs.update_layout(title=dict(text="By Sector", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
                             **base_layout(280), legend=dict(font=dict(family=C["font"], size=10, color="#64748b")))
            st.plotly_chart(fs, use_container_width=True)

        with p3:
            qa = pipe_df.groupby("Quarter")["YTD"].mean().dropna().sort_index()
            fq = go.Figure(go.Bar(x=qa.index, y=qa.values,
                                  marker_color=bar_colors(qa.values),
                                  text=[f"{v:+.1f}%" for v in qa.values], textposition="outside",
                                  textfont=dict(family=C["font"], size=11, color=C["text"])))
            fq.add_hline(y=0, line_color="#334155", line_width=1)
            fq.update_layout(title=dict(text="Avg YTD by Quarter", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
                             **base_layout(280, margin=dict(l=10, r=10, t=44, b=30)),
                             xaxis=dict(showgrid=False, tickfont=dict(size=12)),
                             yaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%", tickfont=dict(size=11)))
            st.plotly_chart(fq, use_container_width=True)

        # Scatter
        st.markdown('<div class="section-header">POSITIONING — YTD vs 1Y</div>', unsafe_allow_html=True)
        sdf = pipe_df.dropna(subset=["YTD", "1Y"]).copy()
        sdf["S"] = sdf.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
        sec_colors = ["#3b82f6","#10b981","#f59e0b","#8b5cf6","#ef4444","#06b6d4","#f97316","#84cc16"]
        fscat = go.Figure()
        for i, (sec, grp) in enumerate(sdf.groupby("Sector")):
            fscat.add_trace(go.Scatter(
                x=grp["YTD"], y=grp["1Y"], mode="markers+text", name=sec,
                marker=dict(color=sec_colors[i % len(sec_colors)], size=10, opacity=0.85),
                text=grp["S"], textposition="top center",
                textfont=dict(family=C["font"], size=10, color=C["text"]),
                hovertemplate="<b>%{text}</b><br>YTD: %{x:.1f}%<br>1Y: %{y:.1f}%<extra></extra>",
            ))
        fscat.add_hline(y=0, line_color="#334155", line_width=1)
        fscat.add_vline(x=0, line_color="#334155", line_width=1)
        fscat.update_layout(title=dict(text="Pipeline: YTD vs 1-Year", font=dict(family=C["font"], size=12, color="#64748b"), x=0),
                            **base_layout(420, margin=dict(l=10, r=10, t=44, b=60)),
                            xaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%", title="YTD",
                                       tickfont=dict(size=11)),
                            yaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%", title="1-Year",
                                       tickfont=dict(size=11)),
                            legend=dict(font=dict(family=C["font"], size=10, color="#64748b"), orientation="h", y=-0.2))
        st.plotly_chart(fscat, use_container_width=True)

        # Pipeline table
        st.markdown('<div class="section-header">PIPELINE TABLE</div>', unsafe_allow_html=True)
        pt = pipe_df[["BBG_Ticker", "Name", "Sector", "Quarter"] + PERIODS].copy()
        pt["Ticker"] = pt.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
        pt = pt.drop(columns=["BBG_Ticker"]).rename(columns={"Quarter": "Q"})
        pt = pt[["Ticker", "Name", "Sector", "Q"] + PERIODS]
        styled_pt = (pt.style
                     .applymap(style_pct, subset=PERIODS)
                     .format({p: lambda x: fmt_pct(x) for p in PERIODS})
                     .set_properties(**{"font-family": "IBM Plex Mono", "font-size": "12px"}))
        st.dataframe(styled_pt, use_container_width=True, height=380)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ADD SECURITY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_add:
    st.markdown('<div class="section-header">ADD PIPELINE SECURITY</div>', unsafe_allow_html=True)

    col_f, col_p = st.columns([1, 1])

    with col_f:
        st.markdown("Fill in the form below. Bloomberg ticker is auto-converted to Yahoo Finance format for data fetching.")
        st.markdown("")

        with st.form("add_form", clear_on_submit=True):
            bbg_in = st.text_input("Bloomberg Ticker *", placeholder="e.g. AAPL US Equity, 9988 HK Equity, 6857 JP Equity")
            name_in = st.text_input("Company Name *", placeholder="e.g. Apple Inc")
            q_in = st.selectbox("Target Quarter", ["Q1", "Q2", "Q3"])
            sec_in = st.selectbox("Sector", SECTORS)
            fetch_on = st.checkbox("Fetch return data from Yahoo Finance", value=True)
            add_btn = st.form_submit_button("➕ Add Security", use_container_width=True)

        if add_btn:
            if not bbg_in.strip() or not name_in.strip():
                st.error("Bloomberg ticker and company name are required.")
            elif st.session_state.df is not None and bbg_in.strip() in st.session_state.df["BBG_Ticker"].values:
                st.warning(f"⚠️ {bbg_in.strip()} already exists.")
            else:
                bbg_clean = bbg_in.strip()
                yahoo = bbg_to_yahoo(bbg_clean)
                new_row = {"BBG_Ticker": bbg_clean, "Yahoo_Ticker": yahoo,
                           "Name": name_in.strip(), "Sector": sec_in,
                           "Quarter": q_in, "Is_DR80": False,
                           **{p: None for p in PERIODS}}

                if fetch_on:
                    if yahoo:
                        with st.spinner(f"Fetching {yahoo}…"):
                            rets = fetch_single(yahoo)
                        new_row.update(rets)
                        st.success(f"✓ Fetched data for {yahoo}")
                    else:
                        st.warning("TB Equity tickers are not available on Yahoo Finance — added without returns.")

                st.session_state.df = pd.concat(
                    [st.session_state.df, pd.DataFrame([new_row])], ignore_index=True
                )
                st.success(f"✓ Added **{bbg_clean}** ({name_in.strip()}) → {sec_in} / {q_in}")
                st.rerun()

    with col_p:
        # Live ticker preview
        st.markdown("**Ticker Conversion Preview**")
        preview_bbg = st.text_input("Type a Bloomberg ticker to preview", placeholder="e.g. 9984 JP Equity",
                                    label_visibility="collapsed")
        if preview_bbg.strip():
            py = bbg_to_yahoo(preview_bbg.strip())
            st.markdown(f"""
            <div style="font-family:IBM Plex Mono;font-size:0.8rem;background:#111827;border:1px solid #1e2d4a;border-radius:8px;padding:16px;margin-bottom:16px;">
            <div style="color:#475569;font-size:0.65rem;text-transform:uppercase;margin-bottom:10px;">Conversion Result</div>
            <div style="color:#94a3b8;margin-bottom:6px;">Bloomberg: <span style="color:#e2e8f0">{preview_bbg.strip()}</span></div>
            <div style="color:#94a3b8;">Yahoo Finance: <span style="color:{'#10b981' if py else '#ef4444'}">{py if py else 'N/A (TB Equity / unknown exchange)'}</span></div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("**Current Pipeline**")
        if st.session_state.df is not None:
            pp = st.session_state.df[~st.session_state.df["Is_DR80"]][
                ["BBG_Ticker", "Name", "Sector", "Quarter", "YTD"]].copy()
            pp["Ticker"] = pp["BBG_Ticker"].apply(short_ticker)
            pp["YTD"] = pp["YTD"].apply(fmt_pct)
            pp = pp.drop(columns=["BBG_Ticker"]).rename(columns={"Quarter": "Q"})
            pp = pp[["Ticker", "Name", "Sector", "Q", "YTD"]]
            st.dataframe(pp, use_container_width=True, height=360, hide_index=True)

    # Save section
    st.markdown("---")
    st.markdown('<div class="section-header">SAVE TO EXCEL</div>', unsafe_allow_html=True)
    st.caption("Downloads a new Excel file preserving the original DR80_Tracking.xlsx structure, with updated returns and any new pipeline entries appended under their correct sector.")

    if st.session_state.excel_bytes and st.session_state.df is not None:
        if st.button("Generate Updated Excel", use_container_width=False):
            with st.spinner("Writing Excel…"):
                xl = write_excel(st.session_state.excel_bytes, st.session_state.df)
            st.download_button(
                "⬇ Download Updated DR80_Tracking.xlsx",
                data=xl,
                file_name=f"DR80_Tracking_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("Load a file in the sidebar first.")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<div style="font-family:IBM Plex Mono;font-size:0.6rem;color:#1e2d4a;text-align:center;">KTB SECURITIES · DR OPERATIONS · DR80 TRACKING SYSTEM</div>', unsafe_allow_html=True)
