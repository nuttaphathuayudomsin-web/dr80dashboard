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
.metric-label { font-family:'IBM Plex Mono',monospace; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.1em; color:#475569; margin-bottom:4px; }
.metric-value { font-family:'IBM Plex Mono',monospace; font-size:1.8rem; font-weight:600; color:#e2e8f0; }
.metric-sub { font-size:0.85rem; color:#64748b; margin-top:2px; }
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


@st.cache_data(show_spinner=False)
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
@st.cache_data(show_spinner=False)
def _parse_sheet(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Parse a DR80-structured sheet (sector headers + ticker rows) into a DataFrame."""
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


@st.cache_data(show_spinner=False)
def parse_excel(file_bytes: bytes) -> pd.DataFrame:
    df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name="Current DR80", header=None)
    return _parse_sheet(df_raw)


@st.cache_data(show_spinner=False)
def parse_competitors(file_bytes: bytes) -> pd.DataFrame:
    """Parse the 'Competitors' sheet if it exists, else return empty DataFrame."""
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
        if "Competitors" not in xl.sheet_names:
            return pd.DataFrame()
        df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name="Competitors", header=None)
        df = _parse_sheet(df_raw)
        df["Is_DR80"] = False
        return df
    except Exception:
        return pd.DataFrame()


def match_competitor_to_dr80(comp_bbg: str, comp_sector: str, dr80_df: pd.DataFrame) -> str | None:
    """
    Match a competitor to a DR80 security.
    1. Try exact base-code match (e.g. 'NVDA' in comp matches 'NVDA80 TB Equity' in DR80)
    2. Fall back to sector match — return sector label
    """
    comp_code = short_ticker(comp_bbg).upper()
    # Remove trailing digits/suffix variants
    for row in dr80_df[dr80_df["Is_DR80"]].itertuples():
        dr_code = short_ticker(row.BBG_Ticker).upper()
        # Strip "80" from DR80 ticker for comparison
        dr_base = dr_code.replace("80", "")
        if comp_code == dr_base or comp_code == dr_code:
            return row.BBG_Ticker
    # No exact match — return sector as the group key
    return comp_sector


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


def graduate_to_dr80(df: pd.DataFrame, bbg_tickers: list) -> pd.DataFrame:
    """
    Promote pipeline securities to DR80 status in the DataFrame.
    Transforms e.g. 'MU US Equity' -> 'MU80 TB Equity', clears Quarter, sets Is_DR80=True.
    """
    df = df.copy()
    for bbg in bbg_tickers:
        mask = df["BBG_Ticker"] == bbg
        if not mask.any():
            continue
        # Build new DR80 ticker: take base code, append '80 TB Equity'
        code = bbg.rsplit(" ", 2)[0].strip()
        new_bbg = f"{code}80 TB Equity"
        df.loc[mask, "BBG_Ticker"] = new_bbg
        df.loc[mask, "Yahoo_Ticker"] = None   # TB tickers not on Yahoo
        df.loc[mask, "Is_DR80"] = True
        df.loc[mask, "Quarter"] = None
    return df


def write_excel_graduated(original_bytes: bytes, df: pd.DataFrame,
                           graduated: list) -> bytes:
    """
    Write Excel with graduation applied:
    - For each graduated ticker, find its pipeline row and update:
      col0 → NaN, col1 → new DR80 ticker, col3 → empty
    - Then call normal write_excel logic for return updates.
    """
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(original_bytes))
    ws = wb["Current DR80"]
    raw = pd.read_excel(io.BytesIO(original_bytes), sheet_name="Current DR80", header=None)

    # Build map: old_bbg -> new_bbg for graduated tickers
    grad_map = {}
    for old_bbg in graduated:
        code = old_bbg.rsplit(" ", 2)[0].strip()
        grad_map[old_bbg] = f"{code}80 TB Equity"

    for i, row in raw.iterrows():
        c1 = str(row[1]) if pd.notna(row[1]) else ""
        if c1.strip() in grad_map:
            new_bbg = grad_map[c1.strip()]
            xl_row = i + 1
            ws.cell(row=xl_row, column=1).value = None       # clear short code
            ws.cell(row=xl_row, column=2).value = new_bbg    # new DR80 ticker
            ws.cell(row=xl_row, column=4).value = None       # clear quarter

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    # Now run normal write_excel on top of the updated bytes
    return write_excel(buf.read(), df)


# ── Chart helpers ──────────────────────────────────────────────────────────────
C = {"bg": "rgba(0,0,0,0)", "grid": "#1e2d4a", "text": "#94a3b8",
     "pos": "#10b981", "neg": "#ef4444", "blue": "#3b82f6", "font": "IBM Plex Mono"}

def base_layout(h=350, margin=None):
    m = margin or dict(l=10, r=10, t=50, b=10)
    return dict(paper_bgcolor=C["bg"], plot_bgcolor=C["bg"],
                font=dict(family=C["font"], color=C["text"], size=14),
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

@st.cache_data(show_spinner=False)
def build_top_bottom_chart(data: bytes, period: str):
    df = pd.read_json(io.BytesIO(data))
    df["S"] = df.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
    seen = {}; deduped = []
    for lbl in df["S"]:
        if lbl in seen: seen[lbl]+=1; deduped.append(f"{lbl} ({seen[lbl]})")
        else: seen[lbl]=0; deduped.append(lbl)
    df["S"] = deduped
    df = df.sort_values(period, ascending=False)
    half = min(15, len(df)//2)
    bar_df = pd.concat([df.head(half), df.tail(half)]).drop_duplicates().sort_values(period)
    bar_h = max(320, len(bar_df)*28+60)
    fig = go.Figure(go.Bar(
        x=bar_df[period], y=bar_df["S"].astype(str), orientation="h",
        marker_color=bar_colors(bar_df[period]), marker_line_width=0,
        text=[f"{v:+.1f}%" for v in bar_df[period]], textposition="outside",
        textfont=dict(family=C["font"], size=13, color=C["text"]),
        hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="#334155", line_width=1)
    fig.update_layout(title=dict(text=f"Top & Bottom Performers — {period}", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                      **base_layout(bar_h), xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                      yaxis=dict(showgrid=False,tickfont=dict(size=14),type="category"))
    return fig

@st.cache_data(show_spinner=False)
def build_heatmap(data: bytes, period: str):
    df = pd.read_json(io.BytesIO(data))
    df["S"] = df.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
    df["abs_ytd"] = df["YTD"].abs()
    df = df.dropna(subset=["YTD"]).nlargest(20, "abs_ytd")
    z = df[PERIODS].values
    heat_h = max(380, len(df)*26+60)
    text_vals = [[f"{v:+.0f}%" if not (v is None or np.isnan(v)) else "—" for v in row] for row in z]
    fig = go.Figure(go.Heatmap(
        z=z, x=PERIODS, y=df["S"].tolist(),
        colorscale=[[0.0,"#991b1b"],[0.3,"#7f1d1d"],[0.5,"#0f172a"],[0.7,"#064e3b"],[1.0,"#059669"]],
        zmid=0, text=text_vals, texttemplate="%{text}",
        textfont=dict(family=C["font"],size=13,color="#e2e8f0"),
        hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
        colorbar=dict(title=dict(text="Return %",font=dict(family=C["font"],size=13,color="#64748b")),
                      tickfont=dict(family=C["font"],size=13,color="#94a3b8"),
                      ticksuffix="%",thickness=14,len=0.9,tickvals=[-100,-50,0,50,100]),
    ))
    fig.update_layout(title=dict(text="Multi-Period Heatmap — Top 20 by |YTD|",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                      **base_layout(heat_h,margin=dict(l=10,r=80,t=50,b=10)),
                      xaxis=dict(showgrid=False,tickfont=dict(size=14,color="#94a3b8"),side="bottom"),
                      yaxis=dict(showgrid=False,autorange="reversed",tickfont=dict(size=14,color="#e2e8f0")))
    return fig


# ── Session state ──────────────────────────────────────────────────────────────
for key, default in [("df", None), ("excel_bytes", None),
                     ("last_refresh", None), ("source_label", None),
                     ("competitors_df", None), ("graduated", [])]:
    if key not in st.session_state:
        st.session_state[key] = default

# Auto-load default file
if st.session_state.df is None and os.path.exists(DEFAULT_FILE):
    with open(DEFAULT_FILE, "rb") as f:
        b = f.read()
    st.session_state.excel_bytes = b
    st.session_state.df = parse_excel(b)
    st.session_state.competitors_df = parse_competitors(b)
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
        st.session_state.df = parse_excel(b)
        st.session_state.competitors_df = parse_competitors(b)
        st.session_state.source_label = uploaded.name
        st.session_state.last_refresh = None
        st.session_state.graduated = []
        comp_count = len(st.session_state.competitors_df)
        msg = f"✓ Loaded {uploaded.name}"
        if comp_count:
            msg += f" · {comp_count} competitors"
        st.success(msg)

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
df_all = st.session_state.df
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
tab_dash, tab_sector, tab_pipeline, tab_competitors, tab_add = st.tabs([
    "📊  Dashboard", "🔬  Sector Analysis", "🔭  Pipeline", "⚔️  Competitors", "➕  Add Security"
])

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

    pv = filt[period].dropna()
    pos_n = int((pv >= 0).sum()); neg_n = int((pv < 0).sum())
    avg_r = float(pv.mean()) if len(pv) else 0.0
    hit_r = pos_n / len(pv) * 100 if len(pv) else 0.0
    best  = filt.loc[filt[period].idxmax()] if len(pv) else None
    worst = filt.loc[filt[period].idxmin()] if len(pv) else None

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Securities</div>
        <div class="metric-value">{len(filt)}</div>
        <div class="metric-sub">DR80: {filt['Is_DR80'].sum()} · Pipeline: {(~filt['Is_DR80']).sum()}</div></div>""", unsafe_allow_html=True)
    with k2:
        vc = C["pos"] if avg_r >= 0 else C["neg"]
        st.markdown(f"""<div class="metric-card {'green' if avg_r>=0 else 'red'}"><div class="metric-label">Avg Return ({period})</div>
        <div class="metric-value" style="color:{vc}">{fmt_pct(avg_r)}</div>
        <div class="metric-sub">{pos_n} pos · {neg_n} neg</div></div>""", unsafe_allow_html=True)
    with k3:
        bt = display_label(best["BBG_Ticker"], best["Name"]) if best is not None else "—"
        st.markdown(f"""<div class="metric-card green"><div class="metric-label">Best ({period})</div>
        <div class="metric-value" style="color:#10b981">{fmt_pct(best[period] if best is not None else None)}</div>
        <div class="metric-sub">{bt}</div></div>""", unsafe_allow_html=True)
    with k4:
        wt = display_label(worst["BBG_Ticker"], worst["Name"]) if worst is not None else "—"
        st.markdown(f"""<div class="metric-card red"><div class="metric-label">Worst ({period})</div>
        <div class="metric-value" style="color:#ef4444">{fmt_pct(worst[period] if worst is not None else None)}</div>
        <div class="metric-sub">{wt}</div></div>""", unsafe_allow_html=True)
    with k5:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Win Rate</div>
        <div class="metric-value">{hit_r:.0f}%</div>
        <div class="metric-sub">+ve return for {period}</div></div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-header">BREAKDOWN</div>', unsafe_allow_html=True)
    pie1, pie2, pie3 = st.columns(3)

    with pie1:
        # By Sector donut
        sec_counts = filt["Sector"].value_counts()
        sec_colors_pie = ["#3b82f6","#10b981","#f59e0b","#8b5cf6","#ef4444","#06b6d4","#f97316","#84cc16"]
        fig_ps = go.Figure(go.Pie(
            labels=sec_counts.index, values=sec_counts.values, hole=0.55,
            marker=dict(colors=sec_colors_pie[:len(sec_counts)]),
            textfont=dict(family=C["font"], size=14),
            hovertemplate="<b>%{label}</b><br>%{value} securities<br>%{percent}<extra></extra>",
        ))
        fig_ps.update_layout(title=dict(text="By Sector", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                             **base_layout(300), legend=dict(font=dict(family=C["font"],size=13,color="#94a3b8")))
        st.plotly_chart(fig_ps, use_container_width=True)

    with pie2:
        # By Type donut (DR80 vs Pipeline)
        type_counts = filt["Is_DR80"].value_counts()
        fig_pt = go.Figure(go.Pie(
            labels=["DR80" if k else "Pipeline" for k in type_counts.index],
            values=type_counts.values, hole=0.55,
            marker=dict(colors=["#3b82f6","#f59e0b"]),
            textfont=dict(family=C["font"], size=14),
            hovertemplate="<b>%{label}</b><br>%{value}<br>%{percent}<extra></extra>",
        ))
        fig_pt.update_layout(title=dict(text="DR80 vs Pipeline", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                             **base_layout(300),
                             legend=dict(font=dict(family=C["font"],size=13,color="#94a3b8")),
                             annotations=[dict(text=f"<b>{len(filt)}</b><br>total", x=0.5, y=0.5,
                                               showarrow=False, font=dict(family=C["font"],size=16,color="#e2e8f0"))])
        st.plotly_chart(fig_pt, use_container_width=True)

    with pie3:
        # By Vintage (Quarter) donut
        vtg = filt["Quarter"].value_counts().sort_index()
        if len(vtg):
            fig_pv = go.Figure(go.Pie(
                labels=vtg.index, values=vtg.values, hole=0.55,
                marker=dict(colors=["#3b82f6","#10b981","#f59e0b","#8b5cf6"]),
                textfont=dict(family=C["font"], size=14),
                hovertemplate="<b>%{label}</b><br>%{value} securities<br>%{percent}<extra></extra>",
            ))
            fig_pv.update_layout(title=dict(text="By Vintage (Quarter)", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                                 **base_layout(300), legend=dict(font=dict(family=C["font"],size=13,color="#94a3b8")))
            st.plotly_chart(fig_pv, use_container_width=True)
        else:
            st.info("No quarter data available.")

    # ── Vintage Analysis ───────────────────────────────────────────────────────
    st.markdown('<div class="section-header">VINTAGE ANALYSIS — PERFORMANCE BY ISSUANCE QUARTER</div>', unsafe_allow_html=True)
    vtg_df = filt.dropna(subset=["Quarter"]).copy()
    if len(vtg_df):
        vtg_df["Label"] = vtg_df.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)

    if len(vtg_df) == 0:
        st.info("No quarter/vintage data available.")
    else:
        va1, va2 = st.columns([2, 3])
        with va1:
            # Bar: avg return per period per vintage
            vtg_avgs = vtg_df.groupby("Quarter")[PERIODS].mean()
            vtg_avgs = vtg_avgs.sort_index()
            fig_va = go.Figure()
            bar_colors_vtg = ["#3b82f6","#10b981","#f59e0b","#8b5cf6"]
            for i, (qtr, row) in enumerate(vtg_avgs.iterrows()):
                fig_va.add_trace(go.Bar(
                    name=qtr, x=PERIODS, y=row.values,
                    marker_color=bar_colors_vtg[i % len(bar_colors_vtg)],
                    text=[f"{v:+.0f}%" if not np.isnan(v) else "" for v in row.values],
                    textposition="outside",
                    textfont=dict(family=C["font"], size=13),
                    hovertemplate=f"<b>{qtr}</b><br>%{{x}}: %{{y:.1f}}%<extra></extra>",
                ))
            fig_va.add_hline(y=0, line_color="#334155", line_width=1)
            fig_va.update_layout(
                title=dict(text="Avg Return by Vintage & Period", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                **base_layout(360, margin=dict(l=10,r=10,t=50,b=10)),
                barmode="group",
                xaxis=dict(showgrid=False, tickfont=dict(size=14)),
                yaxis=dict(showgrid=True, gridcolor=C["grid"], ticksuffix="%", tickfont=dict(size=14)),
                legend=dict(font=dict(family=C["font"],size=13,color="#94a3b8"), orientation="h", y=1.12),
            )
            st.plotly_chart(fig_va, use_container_width=True)

        with va2:
            # Heatmap: each security row, sorted by quarter then YTD
            vtg_heat = vtg_df.sort_values(["Quarter","YTD"], ascending=[True, False])
            vtg_heat = vtg_heat[["Label","Quarter"]+PERIODS].dropna(subset=["YTD"]).head(20)
            # Label includes quarter prefix for readability
            vtg_heat["RowLabel"] = vtg_heat["Quarter"].astype(str) + "  " + vtg_heat["Label"].astype(str)
            z_v = vtg_heat[PERIODS].values.astype(float)
            text_v = [[f"{v:+.0f}%" if not np.isnan(v) else "—" for v in row] for row in z_v]
            fig_vh = go.Figure(go.Heatmap(
                z=z_v, x=PERIODS, y=vtg_heat["RowLabel"].tolist(),
                colorscale=[[0.0,"#991b1b"],[0.3,"#7f1d1d"],[0.5,"#0f172a"],[0.7,"#064e3b"],[1.0,"#059669"]],
                zmid=0, text=text_v, texttemplate="%{text}",
                textfont=dict(family=C["font"], size=16, color="#e2e8f0"),
                hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
                colorbar=dict(tickfont=dict(family=C["font"],size=13,color="#94a3b8"), ticksuffix="%",
                              thickness=14, len=0.9,
                              title=dict(text="Return %",font=dict(family=C["font"],size=13,color="#64748b"))),
            ))
            fig_vh.update_layout(
                title=dict(text="Vintage Heatmap — Top 30 by Quarter", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                **base_layout(max(400, len(vtg_heat)*26+60), margin=dict(l=10,r=80,t=50,b=10)),
                xaxis=dict(showgrid=False, tickfont=dict(size=14,color="#94a3b8")),
                yaxis=dict(showgrid=False, autorange="reversed", tickfont=dict(size=13,color="#e2e8f0")),
            )
            st.plotly_chart(fig_vh, use_container_width=True)

    st.markdown('<div class="section-header">PERFORMANCE</div>', unsafe_allow_html=True)
    ca, cb = st.columns([3, 2])

    with ca:
        pdf = filt[["BBG_Ticker","Name",period]].dropna(subset=[period]).copy()
        pdf["S"] = pdf.apply(lambda r: display_label(r["BBG_Ticker"], r["Name"]), axis=1).astype(str)
        seen = {}; deduped = []
        for lbl in pdf["S"]:
            if lbl in seen: seen[lbl]+=1; deduped.append(f"{lbl} ({seen[lbl]})")
            else: seen[lbl]=0; deduped.append(lbl)
        pdf["S"] = deduped
        pdf = pdf.sort_values(period, ascending=False)
        half = min(15, len(pdf)//2)
        bar_df = pd.concat([pdf.head(half), pdf.tail(half)]).drop_duplicates().sort_values(period)
        bar_h = max(320, len(bar_df)*28+60)
        fig = go.Figure(go.Bar(
            x=bar_df[period], y=bar_df["S"].astype(str), orientation="h",
            marker_color=bar_colors(bar_df[period]), marker_line_width=0,
            text=[f"{v:+.1f}%" for v in bar_df[period]], textposition="outside",
            textfont=dict(family=C["font"], size=13, color=C["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
        ))
        fig.add_vline(x=0, line_color="#334155", line_width=1)
        fig.update_layout(title=dict(text=f"Top & Bottom Performers — {period}", font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                          **base_layout(bar_h), xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                          yaxis=dict(showgrid=False,tickfont=dict(size=14),type="category"))
        st.plotly_chart(fig, use_container_width=True)

    with cb:
        sp = filt.groupby("Sector")[period].mean().dropna().sort_values()
        fig2 = go.Figure(go.Bar(
            x=sp.values, y=sp.index, orientation="h",
            marker_color=bar_colors(sp.values), marker_line_width=0,
            text=[f"{v:+.1f}%" for v in sp.values], textposition="outside",
            textfont=dict(family=C["font"],size=13,color=C["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
        ))
        fig2.add_vline(x=0, line_color="#334155", line_width=1)
        fig2.update_layout(title=dict(text=f"Avg by Sector — {period}",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                           **base_layout(420,margin=dict(l=10,r=80,t=44,b=10)),
                           xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                           yaxis=dict(showgrid=False,tickfont=dict(size=14)))
        st.plotly_chart(fig2, use_container_width=True)

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
                           annotation_font=dict(color="#f59e0b",size=10,family=C["font"]))
        fig3.update_layout(title=dict(text=f"Distribution — {period}",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                           **base_layout(340),xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                           yaxis=dict(showgrid=True,gridcolor=C["grid"],tickfont=dict(size=14)))
        st.plotly_chart(fig3, use_container_width=True)

    with cc2:
        hdf = filt[["BBG_Ticker","Name"]+PERIODS].copy()
        hdf["S"] = hdf.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)
        hdf["abs_ytd"] = hdf["YTD"].abs()
        hdf = hdf.dropna(subset=["YTD"]).nlargest(20,"abs_ytd")
        z = hdf[PERIODS].values
        heat_h = max(380, len(hdf)*26+60)
        text_vals = [[f"{v:+.0f}%" if not (v is None or np.isnan(v)) else "—" for v in row] for row in z]
        fig4 = go.Figure(go.Heatmap(
            z=z, x=PERIODS, y=hdf["S"].tolist(),
            colorscale=[[0.0,"#991b1b"],[0.3,"#7f1d1d"],[0.5,"#0f172a"],[0.7,"#064e3b"],[1.0,"#059669"]],
            zmid=0, text=text_vals, texttemplate="%{text}",
            textfont=dict(family=C["font"],size=16,color="#e2e8f0"),
            hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
            colorbar=dict(title=dict(text="Return %",font=dict(family=C["font"],size=13,color="#64748b")),
                          tickfont=dict(family=C["font"],size=13,color="#94a3b8"),
                          ticksuffix="%",thickness=14,len=0.9,tickvals=[-100,-50,0,50,100]),
        ))
        fig4.update_layout(title=dict(text="Multi-Period Heatmap — Top 20 by |YTD|",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                           **base_layout(heat_h,margin=dict(l=10,r=80,t=44,b=10)),
                           xaxis=dict(showgrid=False,tickfont=dict(size=12,color="#94a3b8"),side="bottom"),
                           yaxis=dict(showgrid=False,autorange="reversed",tickfont=dict(size=11,color="#e2e8f0")))
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown('<div class="section-header">SECTOR HEATMAP — AVG & MEDIAN RETURNS</div>', unsafe_allow_html=True)
    _sec_avgs = filt.groupby("Sector")[PERIODS].mean()
    _sec_meds = filt.groupby("Sector")[PERIODS].median()
    z_sec=[]; y_sec=[]
    for sec in sorted(filt["Sector"].unique()):
        z_sec.append(_sec_avgs.loc[sec].values if sec in _sec_avgs.index else [np.nan]*len(PERIODS)); y_sec.append(f"{sec}  avg")
        z_sec.append(_sec_meds.loc[sec].values if sec in _sec_meds.index else [np.nan]*len(PERIODS)); y_sec.append(f"{sec}  med")
    z_sec_arr = np.array(z_sec, dtype=float)
    sec_h = max(300, len(y_sec)*22+60)
    text_sec = [[f"{v:+.0f}%" if not np.isnan(v) else "—" for v in row] for row in z_sec_arr]
    fig_sh2 = go.Figure(go.Heatmap(
        z=z_sec_arr, x=PERIODS, y=y_sec,
        colorscale=[[0.0,"#991b1b"],[0.3,"#7f1d1d"],[0.5,"#0f172a"],[0.7,"#064e3b"],[1.0,"#059669"]],
        zmid=0, text=text_sec, texttemplate="%{text}",
        textfont=dict(family=C["font"],size=16,color="#e2e8f0"),
        hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
        colorbar=dict(title=dict(text="Return %",font=dict(family=C["font"],size=13,color="#64748b")),
                      tickfont=dict(family=C["font"],size=13,color="#94a3b8"),
                      ticksuffix="%",thickness=14,len=0.9,tickvals=[-50,-25,0,25,50]),
    ))
    fig_sh2.update_layout(title=dict(text="Sector Returns by Period (Avg & Median)",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                          **base_layout(sec_h,margin=dict(l=10,r=80,t=44,b=10)),
                          xaxis=dict(showgrid=False,tickfont=dict(size=12,color="#94a3b8")),
                          yaxis=dict(showgrid=False,autorange="reversed",tickfont=dict(size=11,color="#e2e8f0")))
    st.plotly_chart(fig_sh2, use_container_width=True)

    st.markdown('<div class="section-header">SECURITY TABLE</div>', unsafe_allow_html=True)
    sc1, sc2 = st.columns([2,1])
    with sc1: sort_by = st.selectbox("Sort by", ["Name","Sector"]+PERIODS, index=2, label_visibility="collapsed")
    with sc2: asc = st.checkbox("Ascending", value=False)
    tbl = filt[["BBG_Ticker","Name","Sector","Is_DR80","Quarter"]+PERIODS].copy()
    tbl["Ticker"] = tbl.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)
    tbl["Type"]   = tbl["Is_DR80"].map({True:"DR80",False:"Pipeline"})
    tbl = tbl.drop(columns=["BBG_Ticker","Is_DR80"]).rename(columns={"Quarter":"Q"})
    tbl = tbl[["Ticker","Name","Sector","Type","Q"]+PERIODS].sort_values(sort_by,ascending=asc,na_position="last")
    st.dataframe(tbl.style.applymap(style_pct,subset=PERIODS)
                 .format({p: lambda x: fmt_pct(x) for p in PERIODS})
                 .set_properties(**{"font-family":"IBM Plex Mono","font-size":"12px"}),
                 use_container_width=True, height=430)
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
# TAB 2 — SECTOR ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_sector:
    st.markdown('<div class="section-header">SECTOR DEEP-DIVE</div>', unsafe_allow_html=True)
    sel_sector = st.selectbox("Select sector", sorted(df_all["Sector"].unique()),
                              label_visibility="collapsed", key="sector_drill")
    sec_df = df_all[df_all["Sector"]==sel_sector].copy()

    # DR80 / Pipeline filter
    sec_universe = st.radio("Universe", ["All", "DR80 Only", "Pipeline Only"],
                            horizontal=True, label_visibility="collapsed", key="sector_universe")
    if sec_universe == "DR80 Only":
        sec_df = sec_df[sec_df["Is_DR80"]]
    elif sec_universe == "Pipeline Only":
        sec_df = sec_df[~sec_df["Is_DR80"]]

    sec_df["Label"] = sec_df.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)

    sv = sec_df["YTD"].dropna()
    s1,s2,s3,s4 = st.columns(4)
    with s1:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Securities</div>
        <div class="metric-value">{len(sec_df)}</div>
        <div class="metric-sub">DR80: {sec_df['Is_DR80'].sum()} · Pipeline: {(~sec_df['Is_DR80']).sum()}</div></div>""", unsafe_allow_html=True)
    with s2:
        avg_s = float(sv.mean()) if len(sv) else 0
        vc2 = C["pos"] if avg_s>=0 else C["neg"]
        st.markdown(f"""<div class="metric-card {'green' if avg_s>=0 else 'red'}"><div class="metric-label">Avg YTD Return</div>
        <div class="metric-value" style="color:{vc2}">{fmt_pct(avg_s)}</div>
        <div class="metric-sub">Median: {fmt_pct(float(sv.median()) if len(sv) else None)}</div></div>""", unsafe_allow_html=True)
    with s3:
        if len(sv):
            bs = sec_df.loc[sec_df["YTD"].idxmax()]
            st.markdown(f"""<div class="metric-card green"><div class="metric-label">Best YTD</div>
            <div class="metric-value" style="color:#10b981">{fmt_pct(bs['YTD'])}</div>
            <div class="metric-sub">{bs['Label']}</div></div>""", unsafe_allow_html=True)
    with s4:
        if len(sv):
            ws2 = sec_df.loc[sec_df["YTD"].idxmin()]
            st.markdown(f"""<div class="metric-card red"><div class="metric-label">Worst YTD</div>
            <div class="metric-value" style="color:#ef4444">{fmt_pct(ws2['YTD'])}</div>
            <div class="metric-sub">{ws2['Label']}</div></div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-header">RETURNS BY SECURITY</div>', unsafe_allow_html=True)
    sec_period = st.select_slider("Period", PERIODS, value="YTD", label_visibility="collapsed", key="sector_period")
    sc_a, sc_b = st.columns([3,2])

    with sc_a:
        bar_sec = sec_df[["Label","Name",sec_period]].dropna(subset=[sec_period]).sort_values(sec_period)
        fig_sb = go.Figure(go.Bar(
            x=bar_sec[sec_period], y=bar_sec["Label"].astype(str), orientation="h",
            marker_color=bar_colors(bar_sec[sec_period]), marker_line_width=0,
            text=[f"{v:+.1f}%" for v in bar_sec[sec_period]], textposition="outside",
            textfont=dict(family=C["font"],size=13,color=C["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
        ))
        fig_sb.add_vline(x=0, line_color="#334155", line_width=1)
        fig_sb.update_layout(title=dict(text=f"{sel_sector} — {sec_period}",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                             **base_layout(max(300,len(bar_sec)*30+60)),
                             xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                             yaxis=dict(showgrid=False,tickfont=dict(size=14),type="category"))
        st.plotly_chart(fig_sb, use_container_width=True)

    with sc_b:
        pt1, pt2 = st.tabs(["DR80 vs Pipeline", "Quarter Split"])
        with pt1:
            type_counts = sec_df["Is_DR80"].value_counts()
            fp1 = go.Figure(go.Pie(
                labels=["DR80" if k else "Pipeline" for k in type_counts.index],
                values=type_counts.values, hole=0.55,
                marker_colors=["#3b82f6","#f59e0b"],
                textfont=dict(family=C["font"],size=14),
                hovertemplate="<b>%{label}</b><br>%{value}<br>%{percent}<extra></extra>",
            ))
            fp1.update_layout(**base_layout(280),
                              legend=dict(font=dict(family=C["font"],size=13,color="#64748b")),
                              annotations=[dict(text=f"<b>{len(sec_df)}</b><br>total",x=0.5,y=0.5,
                                                showarrow=False,font=dict(family=C["font"],size=16,color="#e2e8f0"))])
            st.plotly_chart(fp1, use_container_width=True)
        with pt2:
            qc2 = sec_df["Quarter"].value_counts().sort_index()
            if len(qc2):
                fp2 = go.Figure(go.Pie(labels=qc2.index, values=qc2.values, hole=0.55,
                                       marker_colors=["#3b82f6","#10b981","#f59e0b","#8b5cf6"],
                                       textfont=dict(family=C["font"],size=14),
                                       hovertemplate="<b>%{label}</b><br>%{value}<extra></extra>"))
                fp2.update_layout(**base_layout(280),legend=dict(font=dict(family=C["font"],size=13,color="#64748b")))
                st.plotly_chart(fp2, use_container_width=True)
            else:
                st.info("No pipeline securities in this sector.")

    st.markdown('<div class="section-header">MULTI-PERIOD HEATMAP</div>', unsafe_allow_html=True)
    heat_sec = sec_df[["Label"]+PERIODS].dropna(subset=["YTD"]).copy()
    z_s = heat_sec[PERIODS].values
    text_hs = [[f"{v:+.0f}%" if not(v is None or np.isnan(v)) else "—" for v in row] for row in z_s]
    fig_sh = go.Figure(go.Heatmap(
        z=z_s, x=PERIODS, y=heat_sec["Label"].astype(str).tolist(),
        colorscale=[[0.0,"#991b1b"],[0.3,"#7f1d1d"],[0.5,"#0f172a"],[0.7,"#064e3b"],[1.0,"#059669"]],
        zmid=0, text=text_hs, texttemplate="%{text}",
        textfont=dict(family=C["font"],size=16,color="#e2e8f0"),
        hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
        colorbar=dict(tickfont=dict(family=C["font"],size=13,color="#94a3b8"),ticksuffix="%",thickness=14,len=0.9,
                      title=dict(text="Return %",font=dict(family=C["font"],size=13,color="#64748b"))),
    ))
    fig_sh.update_layout(title=dict(text=f"{sel_sector} — All Periods",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                         **base_layout(max(260,len(heat_sec)*28+60),margin=dict(l=10,r=80,t=44,b=10)),
                         xaxis=dict(showgrid=False,tickfont=dict(size=14)),
                         yaxis=dict(showgrid=False,autorange="reversed",tickfont=dict(size=14)))
    st.plotly_chart(fig_sh, use_container_width=True)

    st.markdown('<div class="section-header">SECURITY TABLE</div>', unsafe_allow_html=True)
    stbl = sec_df[["Label","Name","Is_DR80","Quarter"]+PERIODS].copy()
    stbl["Type"] = stbl["Is_DR80"].map({True:"DR80",False:"Pipeline"})
    stbl = stbl.drop(columns=["Is_DR80"]).rename(columns={"Label":"Ticker","Quarter":"Q"})
    stbl = stbl[["Ticker","Name","Type","Q"]+PERIODS]
    st.dataframe(stbl.style.applymap(style_pct,subset=PERIODS)
                 .format({p: lambda x: fmt_pct(x) for p in PERIODS})
                 .set_properties(**{"font-family":"IBM Plex Mono","font-size":"12px"}),
                 use_container_width=True, height=400)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    pipe_df_all = df_all[~df_all["Is_DR80"]].copy()
    st.markdown('<div class="section-header">PIPELINE OVERVIEW</div>', unsafe_allow_html=True)
    if len(pipe_df_all) == 0:
        st.info("No pipeline securities found.")
    else:
        pipe_sectors = sorted(pipe_df_all["Sector"].unique())
        pipe_sel_sectors = st.multiselect("Filter by sector", options=pipe_sectors, default=pipe_sectors,
                                          label_visibility="collapsed", placeholder="All sectors",
                                          key="pipe_sector_filter")
        pipe_df = pipe_df_all[pipe_df_all["Sector"].isin(pipe_sel_sectors)] if pipe_sel_sectors else pipe_df_all
        st.caption(f"Showing {len(pipe_df)} of {len(pipe_df_all)} pipeline securities")

        p1,p2,p3 = st.columns(3)
        with p1:
            qc = pipe_df["Quarter"].value_counts().sort_index()
            fp = go.Figure(go.Pie(labels=qc.index, values=qc.values, hole=0.6,
                                  marker_colors=["#3b82f6","#10b981","#f59e0b","#8b5cf6"],
                                  textfont=dict(family=C["font"],size=14),
                                  hovertemplate="<b>%{label}</b><br>%{value}<extra></extra>"))
            fp.update_layout(**base_layout(280),legend=dict(font=dict(family=C["font"],size=13,color="#64748b")),
                             title=dict(text="By Quarter",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                             annotations=[dict(text=f"<b>{len(pipe_df)}</b><br>total",x=0.5,y=0.5,
                                               showarrow=False,font=dict(family=C["font"],size=16,color="#e2e8f0"))])
            st.plotly_chart(fp, use_container_width=True)
        with p2:
            sc = pipe_df["Sector"].value_counts()
            fs = go.Figure(go.Pie(labels=sc.index, values=sc.values, hole=0.5,
                                  textfont=dict(family=C["font"],size=13),
                                  hovertemplate="<b>%{label}</b><br>%{value}<extra></extra>"))
            fs.update_layout(**base_layout(280),legend=dict(font=dict(family=C["font"],size=13,color="#64748b")),
                             title=dict(text="By Sector",font=dict(family=C["font"],size=15,color="#64748b"),x=0))
            st.plotly_chart(fs, use_container_width=True)
        with p3:
            qa = pipe_df.groupby("Quarter")["YTD"].mean().dropna().sort_index()
            fq = go.Figure(go.Bar(x=qa.index, y=qa.values, marker_color=bar_colors(qa.values),
                                  text=[f"{v:+.1f}%" for v in qa.values], textposition="outside",
                                  textfont=dict(family=C["font"],size=13,color=C["text"])))
            fq.add_hline(y=0, line_color="#334155", line_width=1)
            fq.update_layout(**base_layout(280,margin=dict(l=10,r=10,t=44,b=30)),
                             title=dict(text="Avg YTD by Quarter",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                             xaxis=dict(showgrid=False,tickfont=dict(size=14)),
                             yaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)))
            st.plotly_chart(fq, use_container_width=True)

        st.markdown('<div class="section-header">POSITIONING — YTD vs 1Y</div>', unsafe_allow_html=True)
        sdf = pipe_df.dropna(subset=["YTD","1Y"]).copy()
        sdf["S"] = sdf.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)
        sec_colors = ["#3b82f6","#10b981","#f59e0b","#8b5cf6","#ef4444","#06b6d4","#f97316","#84cc16"]
        fscat = go.Figure()
        for i,(sec,grp) in enumerate(sdf.groupby("Sector")):
            fscat.add_trace(go.Scatter(x=grp["YTD"],y=grp["1Y"],mode="markers+text",name=sec,
                marker=dict(color=sec_colors[i%len(sec_colors)],size=10,opacity=0.85),
                text=grp["S"],textposition="top center",
                textfont=dict(family=C["font"],size=13,color=C["text"]),
                hovertemplate="<b>%{text}</b><br>YTD: %{x:.1f}%<br>1Y: %{y:.1f}%<extra></extra>"))
        fscat.add_hline(y=0,line_color="#334155",line_width=1)
        fscat.add_vline(x=0,line_color="#334155",line_width=1)
        fscat.update_layout(title=dict(text="Pipeline: YTD vs 1-Year",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                            **base_layout(420,margin=dict(l=10,r=10,t=44,b=60)),
                            xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",title="YTD",tickfont=dict(size=14)),
                            yaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",title="1-Year",tickfont=dict(size=14)),
                            legend=dict(font=dict(family=C["font"],size=13,color="#64748b"),orientation="h",y=-0.2))
        st.plotly_chart(fscat, use_container_width=True)

        st.markdown('<div class="section-header">PIPELINE TABLE</div>', unsafe_allow_html=True)
        pt = pipe_df[["BBG_Ticker","Name","Sector","Quarter"]+PERIODS].copy()
        pt["Ticker"] = pt.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)
        pt = pt.drop(columns=["BBG_Ticker"]).rename(columns={"Quarter":"Q"})
        pt = pt[["Ticker","Name","Sector","Q"]+PERIODS]
        st.dataframe(pt.style.applymap(style_pct,subset=PERIODS)
                     .format({p: lambda x: fmt_pct(x) for p in PERIODS})
                     .set_properties(**{"font-family":"IBM Plex Mono","font-size":"12px"}),
                     use_container_width=True, height=380)

    # ── Graduate to DR80 ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div class="section-header">🎓 GRADUATE TO DR80</div>', unsafe_allow_html=True)
    st.caption("Select pipeline securities that have launched and promote them to current DR80 status. Their ticker will be converted to the DR80 format (e.g. MU US Equity → MU80 TB Equity).")

    all_pipe = df_all[~df_all["Is_DR80"]].copy()
    if len(all_pipe) == 0:
        st.info("No pipeline securities to graduate.")
    else:
        # Group by quarter for easy selection
        grad_col1, grad_col2 = st.columns([2, 1])
        with grad_col1:
            # Show all pipeline with quarter label for selection
            all_pipe["Display"] = all_pipe.apply(
                lambda r: f"[{r['Quarter'] or '?'}]  {display_label(r['BBG_Ticker'], r['Name'])}  —  {r['Name'][:35]}",
                axis=1
            )
            # Pre-select Q1 by default since that's the launched quarter
            q1_tickers = all_pipe[all_pipe["Quarter"] == "Q1"]["BBG_Ticker"].tolist()
            all_options = all_pipe["BBG_Ticker"].tolist()
            all_displays = dict(zip(all_pipe["BBG_Ticker"], all_pipe["Display"]))

            to_graduate = st.multiselect(
                "Select securities to graduate",
                options=all_options,
                default=q1_tickers,
                format_func=lambda x: all_displays.get(x, x),
                label_visibility="collapsed",
                key="grad_select",
                placeholder="Select securities to promote to DR80..."
            )

        with grad_col2:
            if to_graduate:
                st.markdown(f"""<div class="metric-card green">
                <div class="metric-label">Ready to Graduate</div>
                <div class="metric-value" style="color:#10b981">{len(to_graduate)}</div>
                <div class="metric-sub">securities selected</div></div>""", unsafe_allow_html=True)

                # Preview the ticker transformation
                st.markdown('<div style="font-family:IBM Plex Mono;font-size:0.75rem;color:#475569;margin-top:12px;margin-bottom:6px;">TICKER CONVERSION PREVIEW</div>', unsafe_allow_html=True)
                for bbg in to_graduate[:5]:
                    code = bbg.rsplit(" ", 2)[0].strip()
                    new_ticker = f"{code}80 TB Equity"
                    st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:0.75rem;color:#64748b;margin-bottom:3px;">{bbg} <span style="color:#3b82f6">→</span> <span style="color:#10b981">{new_ticker}</span></div>', unsafe_allow_html=True)
                if len(to_graduate) > 5:
                    st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:0.7rem;color:#334155;">+ {len(to_graduate)-5} more...</div>', unsafe_allow_html=True)

        if to_graduate:
            st.markdown("")
            g1, g2, _ = st.columns([1, 1, 2])
            with g1:
                if st.button("🎓 Graduate Selected to DR80", use_container_width=True, type="primary", key="grad_btn"):
                    # Update in-memory DataFrame
                    new_df = graduate_to_dr80(st.session_state.df, to_graduate)
                    st.session_state.df = new_df
                    st.session_state.graduated = st.session_state.get("graduated", []) + to_graduate
                    st.success(f"✓ Graduated {len(to_graduate)} securities to DR80. Download the updated Excel below to save permanently.")
                    st.rerun()

            with g2:
                # Download updated Excel with graduations baked in
                if st.session_state.excel_bytes and st.session_state.get("graduated"):
                    try:
                        xl_grad = write_excel_graduated(
                            st.session_state.excel_bytes,
                            st.session_state.df,
                            st.session_state.graduated
                        )
                        st.download_button(
                            "⬇ Download with Graduations",
                            data=xl_grad,
                            file_name=f"DR80_Tracking_graduated_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key="grad_download"
                        )
                    except Exception as e:
                        st.error(f"Excel generation failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — COMPETITORS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_competitors:
    comp_df = st.session_state.competitors_df
    if comp_df is None or len(comp_df) == 0:
        st.markdown('<div class="section-header">COMPETITORS</div>', unsafe_allow_html=True)
        st.info("""No **Competitors** sheet found in the Excel file.

Add a sheet named **`Competitors`** to `DR80_Tracking.xlsx` using the same layout as `Current DR80`:
- Column B: Bloomberg ticker · Column C: Company name
- Column D: Label *(optional)* · Columns E–K: YTD, 1M, 3M, 6M, 1Y, 3Y, 5Y
- Use same sector header rows (col B = sector name, col C = "name")

Then re-upload the file.""")
    else:
        dr80_only = df_all[df_all["Is_DR80"]].copy()
        comp_groups = sorted(comp_df["Sector"].unique())

        st.markdown('<div class="section-header">COMPETITOR GROUPS</div>', unsafe_allow_html=True)
        sel_group = st.selectbox("Select group", comp_groups, label_visibility="collapsed", key="comp_group")
        group_df = comp_df[comp_df["Sector"]==sel_group].copy()
        group_df["Label"] = group_df.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)

        # Match DR80 peers: 1) exact base-code match, 2) same sector fallback
        comp_codes = set(short_ticker(t).upper().replace("80","") for t in group_df["BBG_Ticker"])
        dr80_peers = []
        for _, r in dr80_only.iterrows():
            dr_base = short_ticker(r["BBG_Ticker"]).upper().replace("80","")
            if dr_base in comp_codes or r["Sector"] == sel_group:
                dr80_peers.append(r)
        peers_df = pd.DataFrame(dr80_peers) if dr80_peers else pd.DataFrame(columns=df_all.columns)
        if len(peers_df):
            peers_df = peers_df.copy()
            peers_df["Label"] = peers_df.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1).astype(str)

        comp_period = st.select_slider("Period", PERIODS, value="YTD", label_visibility="collapsed", key="comp_period")

        # ── Side-by-side bars ──────────────────────────────────────────────────
        st.markdown('<div class="section-header">DR80 vs COMPETITORS — SIDE BY SIDE</div>', unsafe_allow_html=True)
        cmp_a, cmp_b = st.columns(2)

        with cmp_a:
            n_peers = len(peers_df)
            st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:0.7rem;color:#3b82f6;margin-bottom:8px;">◆ DR80 PEERS ({n_peers})</div>', unsafe_allow_html=True)
            if n_peers:
                pp2 = peers_df[["Label",comp_period]].dropna(subset=[comp_period]).sort_values(comp_period)
                fig_p = go.Figure(go.Bar(
                    x=pp2[comp_period], y=pp2["Label"].astype(str), orientation="h",
                    marker_color=bar_colors(pp2[comp_period]), marker_line_width=0,
                    text=[f"{v:+.1f}%" for v in pp2[comp_period]], textposition="outside",
                    textfont=dict(family=C["font"],size=13),
                    hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>"))
                fig_p.add_vline(x=0,line_color="#334155",line_width=1)
                fig_p.update_layout(title=dict(text=f"DR80 — {comp_period}",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                                    **base_layout(max(280,n_peers*32+80)),
                                    xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                                    yaxis=dict(showgrid=False,tickfont=dict(size=14),type="category"))
                st.plotly_chart(fig_p, use_container_width=True)
            else:
                st.info("No DR80 peers matched for this group.")

        with cmp_b:
            n_comp = len(group_df)
            st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:0.7rem;color:#f59e0b;margin-bottom:8px;">◆ COMPETITORS ({n_comp})</div>', unsafe_allow_html=True)
            cp2 = group_df[["Label",comp_period]].dropna(subset=[comp_period]).sort_values(comp_period)
            fig_c = go.Figure(go.Bar(
                x=cp2[comp_period], y=cp2["Label"].astype(str), orientation="h",
                marker_color="#f59e0b", marker_line_width=0,
                text=[f"{v:+.1f}%" for v in cp2[comp_period]], textposition="outside",
                textfont=dict(family=C["font"],size=13),
                hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>"))
            fig_c.add_vline(x=0,line_color="#334155",line_width=1)
            fig_c.update_layout(title=dict(text=f"Competitors — {comp_period}",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                                **base_layout(max(280,n_comp*32+80)),
                                xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                                yaxis=dict(showgrid=False,tickfont=dict(size=14),type="category"))
            st.plotly_chart(fig_c, use_container_width=True)

        # ── Combined ranked chart ──────────────────────────────────────────────
        st.markdown('<div class="section-header">COMBINED RANKING</div>', unsafe_allow_html=True)
        peers_plot = peers_df[["Label","Name",comp_period]].copy() if len(peers_df) else pd.DataFrame(columns=["Label","Name",comp_period])
        peers_plot["Type"] = "DR80"
        comp_plot2 = group_df[["Label","Name",comp_period]].copy(); comp_plot2["Type"] = "Competitor"
        combined = pd.concat([peers_plot, comp_plot2]).dropna(subset=[comp_period]).sort_values(comp_period)
        type_colors = combined["Type"].map({"DR80":"#3b82f6","Competitor":"#f59e0b"}).tolist()
        fig_comb = go.Figure(go.Bar(
            x=combined[comp_period], y=combined["Label"].astype(str), orientation="h",
            marker_color=type_colors, marker_line_width=0,
            text=[f"{v:+.1f}%" for v in combined[comp_period]], textposition="outside",
            textfont=dict(family=C["font"],size=13),
            customdata=list(zip(combined["Type"],combined["Name"])),
            hovertemplate="<b>%{y}</b><br>%{customdata[0]}<br>%{customdata[1]}<br>%{x:.1f}%<extra></extra>"))
        for lbl, col in [("DR80","#3b82f6"),("Competitor","#f59e0b")]:
            fig_comb.add_trace(go.Bar(x=[None],y=[None],orientation="h",name=lbl,marker_color=col,showlegend=True))
        fig_comb.add_vline(x=0,line_color="#334155",line_width=1)
        fig_comb.update_layout(title=dict(text=f"Combined Ranking — {comp_period}",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                               **base_layout(max(300,len(combined)*28+80),margin=dict(l=10,r=80,t=44,b=10)),
                               xaxis=dict(showgrid=True,gridcolor=C["grid"],ticksuffix="%",tickfont=dict(size=14)),
                               yaxis=dict(showgrid=False,tickfont=dict(size=14),type="category"),
                               legend=dict(font=dict(family=C["font"],size=13,color="#94a3b8"),bgcolor="rgba(0,0,0,0)",
                                           bordercolor="#1e2d4a",orientation="h",x=0,y=1.04),
                               barmode="relative")
        st.plotly_chart(fig_comb, use_container_width=True)

        # ── Heatmap DR80 + Competitors ─────────────────────────────────────────
        st.markdown('<div class="section-header">MULTI-PERIOD HEATMAP — DR80 vs COMPETITORS</div>', unsafe_allow_html=True)
        heat_comp = group_df[["Label"]+PERIODS].dropna(subset=["YTD"]).copy()
        if len(peers_df):
            heat_dr = peers_df[["Label"]+PERIODS].dropna(subset=["YTD"]).copy()
            sep1 = pd.DataFrame([{"Label":"── DR80 PEERS ──",**{p:np.nan for p in PERIODS}}])
            sep2 = pd.DataFrame([{"Label":"── COMPETITORS ──",**{p:np.nan for p in PERIODS}}])
            heat_all = pd.concat([sep1,heat_dr,sep2,heat_comp],ignore_index=True)
        else:
            heat_all = heat_comp
        z_c = heat_all[PERIODS].values.astype(float)
        text_c = [[f"{v:+.0f}%" if not np.isnan(v) else "" for v in row] for row in z_c]
        fig_ch = go.Figure(go.Heatmap(
            z=z_c, x=PERIODS, y=heat_all["Label"].astype(str).tolist(),
            colorscale=[[0.0,"#991b1b"],[0.3,"#7f1d1d"],[0.5,"#0f172a"],[0.7,"#064e3b"],[1.0,"#059669"]],
            zmid=0, text=text_c, texttemplate="%{text}",
            textfont=dict(family=C["font"],size=16,color="#e2e8f0"),
            hovertemplate="<b>%{y}</b> — %{x}<br>%{z:.1f}%<extra></extra>",
            colorbar=dict(tickfont=dict(family=C["font"],size=13,color="#94a3b8"),ticksuffix="%",thickness=14,len=0.9,
                          title=dict(text="Return %",font=dict(family=C["font"],size=13,color="#64748b")))))
        fig_ch.update_layout(title=dict(text=f"{sel_group} — DR80 vs Competitors",font=dict(family=C["font"],size=15,color="#64748b"),x=0),
                             **base_layout(max(280,len(heat_all)*26+60),margin=dict(l=10,r=80,t=44,b=10)),
                             xaxis=dict(showgrid=False,tickfont=dict(size=14)),
                             yaxis=dict(showgrid=False,autorange="reversed",tickfont=dict(size=14)))
        st.plotly_chart(fig_ch, use_container_width=True)

        # ── Competitor table ───────────────────────────────────────────────────
        st.markdown('<div class="section-header">COMPETITOR TABLE</div>', unsafe_allow_html=True)
        ctbl = group_df[["Label","Name","Sector"]+PERIODS].rename(columns={"Label":"Ticker"})
        st.dataframe(ctbl.style.applymap(style_pct,subset=PERIODS)
                     .format({p: lambda x: fmt_pct(x) for p in PERIODS})
                     .set_properties(**{"font-family":"IBM Plex Mono","font-size":"12px"}),
                     use_container_width=True, height=380)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ADD SECURITY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_add:
    st.markdown('<div class="section-header">ADD PIPELINE SECURITY</div>', unsafe_allow_html=True)
    col_f, col_p = st.columns([1,1])

    with col_f:
        st.markdown("Bloomberg ticker is auto-converted to Yahoo Finance format for data fetching.")
        with st.form("add_form", clear_on_submit=True):
            bbg_in  = st.text_input("Bloomberg Ticker *", placeholder="e.g. AAPL US Equity, 9988 HK Equity")
            name_in = st.text_input("Company Name *", placeholder="e.g. Apple Inc")
            q_in    = st.selectbox("Target Quarter", ["Q1","Q2","Q3"])
            sec_in  = st.selectbox("Sector", SECTORS)
            fetch_on = st.checkbox("Fetch return data from Yahoo Finance", value=True)
            add_btn  = st.form_submit_button("➕ Add Security", use_container_width=True)
        if add_btn:
            if not bbg_in.strip() or not name_in.strip():
                st.error("Bloomberg ticker and company name are required.")
            elif st.session_state.df is not None and bbg_in.strip() in st.session_state.df["BBG_Ticker"].values:
                st.warning(f"⚠️ {bbg_in.strip()} already exists.")
            else:
                bbg_clean = bbg_in.strip()
                yahoo = bbg_to_yahoo(bbg_clean)
                new_row = {"BBG_Ticker":bbg_clean,"Yahoo_Ticker":yahoo,"Name":name_in.strip(),
                           "Sector":sec_in,"Quarter":q_in,"Is_DR80":False,**{p:None for p in PERIODS}}
                if fetch_on:
                    if yahoo:
                        with st.spinner(f"Fetching {yahoo}…"):
                            rets = fetch_single(yahoo)
                        new_row.update(rets)
                        st.success(f"✓ Fetched data for {yahoo}")
                    else:
                        st.warning("TB Equity tickers not available on Yahoo Finance — added without returns.")
                st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame([new_row])], ignore_index=True)
                st.success(f"✓ Added **{bbg_clean}** ({name_in.strip()}) → {sec_in} / {q_in}")
                st.rerun()

    with col_p:
        st.markdown("**Ticker Conversion Preview**")
        preview_bbg = st.text_input("", placeholder="e.g. 9984 JP Equity", label_visibility="collapsed")
        if preview_bbg.strip():
            py = bbg_to_yahoo(preview_bbg.strip())
            st.markdown(f"""<div style="font-family:IBM Plex Mono;font-size:0.8rem;background:#111827;border:1px solid #1e2d4a;border-radius:8px;padding:16px;margin-bottom:16px;">
            <div style="color:#475569;font-size:0.65rem;text-transform:uppercase;margin-bottom:10px;">Conversion Result</div>
            <div style="color:#94a3b8;margin-bottom:6px;">Bloomberg: <span style="color:#e2e8f0">{preview_bbg.strip()}</span></div>
            <div style="color:#94a3b8;">Yahoo Finance: <span style="color:{'#10b981' if py else '#ef4444'}">{py or 'N/A (TB Equity)'}</span></div>
            </div>""", unsafe_allow_html=True)
        st.markdown("**Current Pipeline**")
        if st.session_state.df is not None:
            pp = st.session_state.df[~st.session_state.df["Is_DR80"]][["BBG_Ticker","Name","Sector","Quarter","YTD"]].copy()
            pp["Ticker"] = pp.apply(lambda r: display_label(r["BBG_Ticker"],r["Name"]),axis=1)
            pp["YTD"] = pp["YTD"].apply(fmt_pct)
            pp = pp.drop(columns=["BBG_Ticker"]).rename(columns={"Quarter":"Q"})
            st.dataframe(pp[["Ticker","Name","Sector","Q","YTD"]], use_container_width=True, height=360, hide_index=True)

    st.markdown("---")
    st.markdown('<div class="section-header">SAVE TO EXCEL</div>', unsafe_allow_html=True)
    st.caption("Downloads updated Excel preserving original structure with refreshed returns and new pipeline entries.")
    if st.session_state.excel_bytes and st.session_state.df is not None:
        if st.button("Generate Updated Excel"):
            with st.spinner("Writing Excel…"):
                xl = write_excel(st.session_state.excel_bytes, st.session_state.df)
            st.download_button("⬇ Download Updated DR80_Tracking.xlsx", data=xl,
                               file_name=f"DR80_Tracking_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Load a file in the sidebar first.")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<div style="font-family:IBM Plex Mono;font-size:0.6rem;color:#1e2d4a;text-align:center;">KTB SECURITIES · DR OPERATIONS · DR80 TRACKING SYSTEM</div>', unsafe_allow_html=True)
