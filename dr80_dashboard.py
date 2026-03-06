import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import urllib.parse, urllib.request
import re, json, xml.etree.ElementTree as ET
from datetime import datetime, timedelta

st.set_page_config(page_title="Global Market Monitor", page_icon="📈", layout="wide")

st.markdown("""
<style>
    .stMetric { background: #0d1117; border: 1px solid #1c2333; border-radius: 8px; padding: 12px; }
    div[data-testid="stMetricDelta"] svg { display: none; }
    .news-card {
        background: #0d1117; border: 1px solid #1c2333;
        border-left: 3px solid #0080ff; border-radius: 6px;
        padding: 12px 16px; margin-bottom: 10px;
    }
    .news-title { font-size: 14px; font-weight: 600; margin-bottom: 4px; }
    .news-meta  { font-size: 11px; color: #7d8590; }

    /* AI Capex tab styles */
    .capex-card {
        background: #0d1117; border: 1px solid #1c2333;
        border-radius: 10px; padding: 16px; margin-bottom: 12px;
    }
    .capex-label {
        font-size: 10px; letter-spacing: 0.12em; text-transform: uppercase;
        color: #7d8590; font-family: monospace; margin-bottom: 4px;
    }
    .capex-value {
        font-size: 28px; font-weight: 700; font-family: monospace;
        color: #e6edf3;
    }
    .capex-sub { font-size: 12px; color: #7d8590; margin-top: 4px; }
    .signal-up   { color: #3fb950; font-weight: 600; }
    .signal-down { color: #f85149; font-weight: 600; }
    .signal-flat { color: #d29922; font-weight: 600; }
    .section-divider {
        border: none; border-top: 1px solid #1c2333;
        margin: 24px 0 16px 0;
    }
    .ticker-badge {
        display: inline-block; background: rgba(0,128,255,0.12);
        color: #58a6ff; border-radius: 4px;
        padding: 1px 7px; font-size: 11px; margin-right: 4px;
        font-family: monospace;
    }
</style>
""", unsafe_allow_html=True)

# ── INDICES ───────────────────────────────────────────────────────────────────
INDICES = {
    "🇺🇸 Dow Jones":  {"tickers": ["^DJI",       "DIA"]},
    "🇺🇸 S&P 500":    {"tickers": ["^GSPC",      "SPY"]},
    "🇺🇸 NASDAQ":     {"tickers": ["^IXIC",      "QQQ"]},
    "🇰🇷 KOSPI":      {"tickers": ["^KS11",      "EWY"]},
    "🇯🇵 Nikkei 225": {"tickers": ["^N225",      "1321.T", "EWJ"]},
    "🇭🇰 Hang Seng":  {"tickers": ["^HSI",       "2800.HK"]},
    "🇨🇳 Shenzhen":   {"tickers": ["399001.SZ"]},
    "🇨🇳 Shanghai":   {"tickers": ["000001.SS"]},
    "🇹🇭 SET Index":  {"tickers": ["^SET.BK"]},
    "🇻🇳 VN Index":   {"tickers": ["^VNINDEX"]},
}

COMMODITIES = {
    "🛢️ Crude Oil (WTI)": {"tickers": ["CL=F",    "USO"],    "unit": "USD/bbl"},
    "🥇 Gold":             {"tickers": ["GC=F",    "GLD"],    "unit": "USD/oz"},
    "🥈 Silver":           {"tickers": ["SI=F",    "SLV"],    "unit": "USD/oz"},
    "₿  Bitcoin":          {"tickers": ["BTC-USD"],           "unit": "USD"},
    "⛽ Natural Gas":      {"tickers": ["NG=F"],               "unit": "USD/MMBtu"},
    "🥉 Copper":           {"tickers": ["HG=F"],               "unit": "USD/lb"},
}

SECTOR_ETFS = {
    "Technology":            "XLK",
    "Financials":            "XLF",
    "Healthcare":            "XLV",
    "Energy":                "XLE",
    "Consumer Cyclical":     "XLY",
    "Consumer Defensive":    "XLP",
    "Industrials":           "XLI",
    "Basic Materials":       "XLB",
    "Real Estate":           "XLRE",
    "Utilities":             "XLU",
    "Communication":         "XLC",
}

US_STOCKS = {
    "NVDA": ("NVIDIA Corp",           "Technology",   "Semiconductors"),
    "AAPL": ("Apple Inc",             "Technology",   "Consumer Electronics"),
    "MSFT": ("Microsoft Corp",        "Technology",   "Cloud / Software"),
    "GOOGL":("Alphabet Inc",          "Technology",   "Internet / Advertising"),
    "META": ("Meta Platforms",        "Technology",   "Social Media"),
    "AMD":  ("Advanced Micro Devices","Technology",   "Semiconductors"),
    "AVGO": ("Broadcom Inc",          "Technology",   "Semiconductors"),
    "PLTR": ("Palantir Technologies", "Technology",   "AI / Data Analytics"),
    "INTC": ("Intel Corp",            "Technology",   "Semiconductors"),
    "CSCO": ("Cisco Systems",         "Technology",   "Networking"),
    "NOW":  ("ServiceNow",            "Technology",   "Cloud / SaaS"),
    "SNOW": ("Snowflake",             "Technology",   "Cloud / Data"),
    "ORCL": ("Oracle Corp",           "Technology",   "Cloud / Software"),
    "IBM":  ("IBM Corp",              "Technology",   "IT Services"),
    "QCOM": ("Qualcomm",              "Technology",   "Semiconductors"),
    "TXN":  ("Texas Instruments",     "Technology",   "Semiconductors"),
    "MU":   ("Micron Technology",     "Technology",   "Memory Chips"),
    "AMAT": ("Applied Materials",     "Technology",   "Semiconductor Equipment"),
    "LRCX": ("Lam Research",          "Technology",   "Semiconductor Equipment"),
    "KLAC": ("KLA Corp",              "Technology",   "Semiconductor Equipment"),
    "ADBE": ("Adobe Inc",             "Technology",   "Software"),
    "CRM":  ("Salesforce",            "Technology",   "Cloud / SaaS"),
    "INTU": ("Intuit Inc",            "Technology",   "Fintech / Software"),
    "PANW": ("Palo Alto Networks",    "Technology",   "Cybersecurity"),
    "CRWD": ("CrowdStrike",           "Technology",   "Cybersecurity"),
    "TSLA": ("Tesla Inc",             "Consumer Cyclical","Electric Vehicles"),
    "AMZN": ("Amazon.com",            "Consumer Cyclical","E-Commerce / Cloud"),
    "HD":   ("Home Depot",            "Consumer Cyclical","Home Improvement Retail"),
    "MCD":  ("McDonald's Corp",       "Consumer Cyclical","Restaurants"),
    "NKE":  ("Nike Inc",              "Consumer Cyclical","Apparel / Sportswear"),
    "SBUX": ("Starbucks Corp",        "Consumer Cyclical","Restaurants"),
    "TGT":  ("Target Corp",           "Consumer Cyclical","Retail"),
    "BKNG": ("Booking Holdings",      "Consumer Cyclical","Online Travel"),
    "ABNB": ("Airbnb Inc",            "Consumer Cyclical","Online Travel"),
    "LOW":  ("Lowe's Companies",      "Consumer Cyclical","Home Improvement Retail"),
    "WMT":  ("Walmart Inc",           "Consumer Defensive","Retail"),
    "KO":   ("Coca-Cola Co",          "Consumer Defensive","Beverages"),
    "PEP":  ("PepsiCo Inc",           "Consumer Defensive","Beverages"),
    "COST": ("Costco Wholesale",      "Consumer Defensive","Retail"),
    "PG":   ("Procter & Gamble",      "Consumer Defensive","Household Products"),
    "PM":   ("Philip Morris Intl",    "Consumer Defensive","Tobacco"),
    "JPM":  ("JPMorgan Chase",        "Financials",   "Banks"),
    "BAC":  ("Bank of America",       "Financials",   "Banks"),
    "WFC":  ("Wells Fargo",           "Financials",   "Banks"),
    "GS":   ("Goldman Sachs",         "Financials",   "Investment Banking"),
    "MS":   ("Morgan Stanley",        "Financials",   "Investment Banking"),
    "BLK":  ("BlackRock Inc",         "Financials",   "Asset Management"),
    "V":    ("Visa Inc",              "Financials",   "Payment Networks"),
    "MA":   ("Mastercard",            "Financials",   "Payment Networks"),
    "JNJ":  ("Johnson & Johnson",     "Healthcare",   "Pharma / Medical"),
    "UNH":  ("UnitedHealth Group",    "Healthcare",   "Health Insurance"),
    "LLY":  ("Eli Lilly",             "Healthcare",   "Pharma / GLP-1"),
    "PFE":  ("Pfizer Inc",            "Healthcare",   "Pharma"),
    "ABBV": ("AbbVie Inc",            "Healthcare",   "Biopharma"),
    "XOM":  ("ExxonMobil",            "Energy",       "Oil & Gas Integrated"),
    "CVX":  ("Chevron Corp",          "Energy",       "Oil & Gas Integrated"),
    "NFLX": ("Netflix Inc",           "Communication","Streaming"),
    "DIS":  ("Walt Disney Co",        "Communication","Media / Entertainment"),
    "GE":   ("GE Aerospace",          "Industrials",  "Aerospace / Defense"),
    "BA":   ("Boeing Co",             "Industrials",  "Aerospace"),
    "LMT":  ("Lockheed Martin",       "Industrials",  "Defense"),
    "NEE":  ("NextEra Energy",        "Utilities",    "Renewable Energy"),
}

KR_STOCKS = {
    "005930.KS":("Samsung Electronics","Technology",   "Semiconductors / Consumer"),
    "000660.KS":("SK Hynix",           "Technology",   "Memory Chips"),
    "035420.KS":("NAVER Corp",         "Technology",   "Internet / Search"),
    "051910.KS":("LG Chem",            "Basic Materials","Batteries / Chemicals"),
    "006400.KS":("Samsung SDI",        "Basic Materials","Battery / Energy Storage"),
    "035720.KS":("Kakao Corp",         "Technology",   "Social / Fintech"),
    "000270.KS":("Kia Corp",           "Consumer Cyclical","Automobiles"),
    "068270.KS":("Celltrion",          "Healthcare",   "Biotech / Biosimilars"),
    "207940.KS":("Samsung Biologics",  "Healthcare",   "Contract Manufacturing"),
    "005380.KS":("Hyundai Motor",      "Consumer Cyclical","Automobiles"),
}

JP_STOCKS = {
    "7203.T": ("Toyota Motor",         "Consumer Cyclical","Automobiles"),
    "9984.T": ("SoftBank Group",       "Technology",   "Venture / Telecom"),
    "6861.T": ("Keyence Corp",         "Technology",   "Industrial Automation"),
    "8306.T": ("Mitsubishi UFJ",       "Financials",   "Banks"),
    "6758.T": ("Sony Group",           "Consumer Cyclical","Electronics / Gaming"),
    "9432.T": ("NTT Corp",             "Communication","Telecom"),
    "7974.T": ("Nintendo",             "Consumer Cyclical","Gaming"),
    "8035.T": ("Tokyo Electron",       "Technology",   "Semiconductor Equipment"),
    "4502.T": ("Takeda Pharmaceutical","Healthcare",   "Pharma"),
    "6501.T": ("Hitachi Ltd",          "Industrials",  "Conglomerate / IT"),
}

HK_STOCKS = {
    "0700.HK":("Tencent Holdings",    "Technology",   "Gaming / Social"),
    "9988.HK":("Alibaba Group",        "Technology",   "E-Commerce / Cloud"),
    "1299.HK":("AIA Group",            "Financials",   "Life Insurance"),
    "0005.HK":("HSBC Holdings",        "Financials",   "Banks"),
    "2318.HK":("Ping An Insurance",    "Financials",   "Insurance"),
    "3690.HK":("Meituan",              "Consumer Cyclical","Food Delivery"),
    "1810.HK":("Xiaomi Corp",          "Technology",   "Consumer Electronics"),
    "9618.HK":("JD.com",               "Consumer Cyclical","E-Commerce"),
}

SH_STOCKS = {
    "600519.SS":("Kweichow Moutai",    "Consumer Defensive","Premium Spirits"),
    "601318.SS":("Ping An Insurance",  "Financials",   "Insurance"),
    "600036.SS":("China Merchants Bank","Financials",  "Banks"),
    "600900.SS":("Yangtze Power",      "Utilities",    "Hydropower"),
    "600276.SS":("Hengrui Medicine",   "Healthcare",   "Pharma / Oncology"),
    "601012.SS":("Longi Green Energy", "Utilities",    "Solar Energy"),
}

SZ_STOCKS = {
    "000858.SZ":("Wuliangye Yibin",   "Consumer Defensive","Premium Spirits"),
    "000333.SZ":("Midea Group",        "Consumer Cyclical","Home Appliances"),
    "002594.SZ":("BYD Co",             "Consumer Cyclical","EV / Batteries"),
    "300750.SZ":("CATL",               "Basic Materials","EV Batteries"),
    "002415.SZ":("Hikvision",          "Technology",   "Video Surveillance / AI"),
    "300760.SZ":("Mindray Medical",    "Healthcare",   "Medical Devices"),
}

TH_STOCKS = {
    "PTT.BK":    ("PTT PCL",           "Energy",       "Oil & Gas (State)"),
    "ADVANC.BK": ("Advanced Info Svc", "Communication","Telecom"),
    "CPALL.BK":  ("CP All PCL",        "Consumer Defensive","Convenience Retail"),
    "AOT.BK":    ("Airports of Thailand","Industrials","Airport Operator"),
    "KBANK.BK":  ("Kasikornbank",      "Financials",   "Banks"),
    "SCB.BK":    ("SCB Group",         "Financials",   "Banks"),
    "GULF.BK":   ("Gulf Energy Dev",   "Utilities",    "Power Generation"),
    "BDMS.BK":   ("Bangkok Dusit Med", "Healthcare",   "Private Hospitals"),
}

VN_STOCKS = {
    "VCB.VN":    ("Vietcombank",       "Financials",   "Banks"),
    "VHM.VN":    ("Vinhomes",          "Real Estate",  "Property Developer"),
    "HPG.VN":    ("Hoa Phat Group",    "Basic Materials","Steel / Industrial"),
    "TCB.VN":    ("Techcombank",       "Financials",   "Banks"),
    "FPT.VN":    ("FPT Corp",          "Technology",   "IT Services / Telecom"),
    "MWG.VN":    ("Mobile World",      "Consumer Cyclical","Electronics Retail"),
}

MARKET_STOCK_MAP = {
    "🇺🇸 US Market":        US_STOCKS,
    "🇰🇷 Korea (KRX)":      KR_STOCKS,
    "🇯🇵 Japan (TSE)":      JP_STOCKS,
    "🇭🇰 Hong Kong":        HK_STOCKS,
    "🇨🇳 Shanghai":         SH_STOCKS,
    "🇨🇳 Shenzhen":         SZ_STOCKS,
    "🇹🇭 Thailand (SET)":   TH_STOCKS,
    "🇻🇳 Vietnam (HoSE)":   VN_STOCKS,
}

PERIOD_MAP = {
    "1 Day":    ("2d",    "1D"),
    "1 Week":   ("7d",    "1W"),
    "1 Month":  ("35d",   "1M"),
    "3 Months": ("100d",  "3M"),
    "6 Months": ("190d",  "6M"),
    "YTD":      ("ytd",   "YTD"),
    "1 Year":   ("370d",  "1Y"),
    "3 Years":  ("1100d", "3Y"),
    "5 Years":  ("1830d", "5Y"),
}

NEWS_CATEGORIES = {
    "🌍 All Markets":          "stock market investing economy global",
    "💰 Finance & Economy":    "finance economy central bank interest rates inflation GDP",
    "⚔️ Geopolitics & War":    "war conflict geopolitics military sanctions Ukraine Russia Middle East",
    "🏛️ Politics & Policy":    "politics government policy election trade tariff regulation",
    "💻 Technology & AI":      "technology AI artificial intelligence semiconductor chips",
    "🛢️ Energy & Commodities": "oil gas energy commodities gold copper lithium",
    "🏦 Banking & Crypto":     "banking crypto bitcoin cryptocurrency Fed ECB",
    "🏭 Industry & Trade":     "supply chain manufacturing trade export tariff",
}

# ═════════════════════════════════════════════════════════════════════════════
# AI CAPEX TRACKER — DATA LAYER
# ═════════════════════════════════════════════════════════════════════════════

# ── Mag7 Capex — hardcoded from latest earnings, enriched via yfinance ────────
# Source: company 10-Q/10-K filings. Updated each earnings season.
# Capex = Purchase of Property, Plant & Equipment (cash flow statement)
# All figures in USD billions
MAG7_CAPEX_HISTORY = {
    "MSFT": {
        "name": "Microsoft", "color": "#00a2ed",
        "quarters": [
            # (label, fiscal_label, capex_bn)
            ("Q2'25", "Oct-Dec 2024", 22.6),
            ("Q1'25", "Jul-Sep 2024", 20.0),
            ("Q4'24", "Apr-Jun 2024", 19.0),
            ("Q3'24", "Jan-Mar 2024", 14.0),
            ("Q2'24", "Oct-Dec 2023", 11.5),
            ("Q1'24", "Jul-Sep 2023",  9.9),
            ("Q4'23", "Apr-Jun 2023",  8.9),
            ("Q3'23", "Jan-Mar 2023",  7.8),
        ],
        "guidance": "~$80B for full FY2025",
        "focus": "Azure AI infrastructure, data centers",
    },
    "GOOG": {
        "name": "Alphabet", "color": "#4285f4",
        "quarters": [
            ("Q3'24", "Jul-Sep 2024", 13.1),
            ("Q2'24", "Apr-Jun 2024", 13.2),
            ("Q1'24", "Jan-Mar 2024", 12.0),
            ("Q4'23", "Oct-Dec 2023", 11.0),
            ("Q3'23", "Jul-Sep 2023",  8.1),
            ("Q2'23", "Apr-Jun 2023",  6.9),
            ("Q1'23", "Jan-Mar 2023",  6.3),
            ("Q4'22", "Oct-Dec 2022",  7.2),
        ],
        "guidance": "~$75B for full year 2025",
        "focus": "TPU clusters, Google Cloud, Gemini infra",
    },
    "META": {
        "name": "Meta Platforms", "color": "#0668E1",
        "quarters": [
            ("Q3'24", "Jul-Sep 2024",  9.2),
            ("Q2'24", "Apr-Jun 2024",  8.5),
            ("Q1'24", "Jan-Mar 2024",  6.7),
            ("Q4'23", "Oct-Dec 2023",  6.8),
            ("Q3'23", "Jul-Sep 2023",  6.7),
            ("Q2'23", "Apr-Jun 2023",  6.4),
            ("Q1'23", "Jan-Mar 2023",  6.5),
            ("Q4'22", "Oct-Dec 2022",  9.0),
        ],
        "guidance": "$60–65B for 2025",
        "focus": "Custom MTIA chips, data centers for Llama",
    },
    "AMZN": {
        "name": "Amazon", "color": "#ff9900",
        "quarters": [
            ("Q3'24", "Jul-Sep 2024", 22.6),
            ("Q2'24", "Apr-Jun 2024", 17.6),
            ("Q1'24", "Jan-Mar 2024", 14.9),
            ("Q4'23", "Oct-Dec 2023", 14.1),
            ("Q3'23", "Jul-Sep 2023", 12.5),
            ("Q2'23", "Apr-Jun 2023", 12.5),
            ("Q1'23", "Jan-Mar 2023", 14.1),
            ("Q4'22", "Oct-Dec 2022", 16.4),
        ],
        "guidance": "$75B+ for 2025 (AWS AI)",
        "focus": "AWS data centers, Trainium/Inferentia chips",
    },
    "AAPL": {
        "name": "Apple", "color": "#a2aaad",
        "quarters": [
            ("Q4'24", "Jul-Sep 2024",  2.9),
            ("Q3'24", "Apr-Jun 2024",  2.8),
            ("Q2'24", "Jan-Mar 2024",  2.5),
            ("Q1'24", "Oct-Dec 2023",  2.5),
            ("Q4'23", "Jul-Sep 2023",  2.7),
            ("Q3'23", "Apr-Jun 2023",  2.8),
            ("Q2'23", "Jan-Mar 2023",  2.3),
            ("Q1'23", "Oct-Dec 2022",  3.0),
        ],
        "guidance": "No specific AI infra guidance",
        "focus": "Apple Silicon, on-device AI (Apple Intelligence)",
    },
    "NVDA": {
        "name": "NVIDIA", "color": "#76b900",
        "quarters": [
            ("Q3'25", "Aug-Oct 2024",  0.5),
            ("Q2'25", "May-Jul 2024",  0.4),
            ("Q1'25", "Feb-Apr 2024",  0.3),
            ("Q4'24", "Nov-Jan 2024",  0.4),
            ("Q3'24", "Aug-Oct 2023",  0.3),
            ("Q2'24", "May-Jul 2023",  0.2),
            ("Q1'24", "Feb-Apr 2023",  0.2),
            ("Q4'23", "Nov-Jan 2023",  0.2),
        ],
        "guidance": "Asset-light — capex stays low, revenue is the signal",
        "focus": "Fabless: TSMC manufactures. Track revenue not capex.",
    },
    "TSLA": {
        "name": "Tesla", "color": "#cc0000",
        "quarters": [
            ("Q3'24", "Jul-Sep 2024",  3.5),
            ("Q2'24", "Apr-Jun 2024",  2.7),
            ("Q1'24", "Jan-Mar 2024",  2.8),
            ("Q4'23", "Oct-Dec 2023",  2.3),
            ("Q3'23", "Jul-Sep 2023",  2.5),
            ("Q2'23", "Apr-Jun 2023",  2.1),
            ("Q1'23", "Jan-Mar 2023",  2.2),
            ("Q4'22", "Oct-Dec 2022",  1.9),
        ],
        "guidance": "$10–11B for 2025 (Giga factories + Dojo)",
        "focus": "Dojo supercomputer, Gigafactory expansion",
    },
}

# ── Infrastructure proxies — pure-play beneficiaries ─────────────────────────
INFRA_PROXIES = {
    "SMCI":  ("Super Micro Computer", "Server assembly for AI data centers"),
    "ANET":  ("Arista Networks",      "Data center networking — 400G/800G"),
    "VRT":   ("Vertiv Holdings",      "Power & cooling for data centers"),
    "EQIX":  ("Equinix",              "Colocation — neutral data center REIT"),
    "DLR":   ("Digital Realty",       "Data center REIT"),
    "CEG":   ("Constellation Energy", "Nuclear power — AI data center PPAs"),
    "VST":   ("Vistra Corp",          "Power gen — data center electricity"),
    "NRG":   ("NRG Energy",           "Power gen — AI electricity demand"),
    "DELL":  ("Dell Technologies",    "AI server systems (PowerEdge)"),
    "HPE":   ("Hewlett Packard Ent",  "AI servers & networking"),
}

# ── AI Capex helpers ──────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def fetch_arxiv_ai_volume(weeks: int = 16) -> pd.DataFrame:
    """
    Query ArXiv API for weekly AI paper submission counts.
    Uses cs.AI + cs.LG + stat.ML categories.
    Returns DataFrame with week-ending date and paper count.
    """
    rows = []
    today = datetime.today()
    for w in range(weeks, 0, -1):
        week_end   = today - timedelta(weeks=w-1)
        week_start = week_end - timedelta(days=6)
        s = week_start.strftime("%Y%m%d")
        e = week_end.strftime("%Y%m%d")
        query = f"cat:cs.AI+OR+cat:cs.LG+OR+cat:stat.ML&submittedDate=[{s}+TO+{e}]"
        url   = f"http://export.arxiv.org/api/query?search_query={query}&max_results=1&sortBy=submittedDate"
        try:
            req  = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=10)
            xml  = resp.read().decode("utf-8")
            ns   = {"a": "http://www.w3.org/2005/Atom",
                    "os": "http://a9.com/-/spec/opensearch/1.1/"}
            root  = ET.fromstring(xml)
            total = root.find("os:totalResults", ns)
            count = int(total.text) if total is not None else 0
            rows.append({"week_end": week_end.strftime("%Y-%m-%d"), "papers": count})
        except Exception:
            rows.append({"week_end": week_end.strftime("%Y-%m-%d"), "papers": None})
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600)
def fetch_ercot_load_queue() -> dict:
    """
    Fetch ERCOT large load interconnection queue summary.
    Returns dict with total GW queued and recent additions.
    Uses ERCOT's public Generator Interconnection Status report.
    """
    try:
        url = "https://www.ercot.com/misapp/GetReports.do?reportTypeId=15933&reportTitle=GIS+Report&showHTMLView=&mimicKey"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=10)
        html = resp.read().decode("utf-8", errors="ignore")
        # Parse the GW figures from the queue table
        gw_matches = re.findall(r'(\d[\d,]*\.?\d*)\s*MW', html)
        if gw_matches:
            total_mw = sum(float(v.replace(",","")) for v in gw_matches[:20])
            return {"total_gw": round(total_mw / 1000, 1), "status": "live", "source": "ERCOT GIS"}
    except Exception:
        pass
    # Fallback: last known public figures (updated manually each quarter)
    return {
        "total_gw": 304.0,
        "large_load_gw": 175.0,  # specifically "large load" (data centers, industrial)
        "yoy_change_pct": 42.0,
        "status": "cached",
        "as_of": "Q4 2024",
        "source": "ERCOT GIS Report (cached)",
        "note": "175 GW of large load requests queued — ~3× Texas current peak demand"
    }


@st.cache_data(ttl=3600)
def fetch_taiwan_exports() -> pd.DataFrame:
    """
    Taiwan exports to US — proxy for semiconductor/server shipment volumes.
    Uses Taiwan Ministry of Finance trade statistics (public).
    Falls back to hardcoded recent data.
    """
    try:
        url = "https://portal.customs.gov.tw/etcd-client-war/DownloadServlet?type=xml&fileName=AS4C010.xml"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=10)
        # Try to parse
        data = resp.read()
        # If successful, parse XML... (complex, fall through to hardcoded)
    except Exception:
        pass

    # Hardcoded from Ministry of Finance press releases (USD billions, exports to USA)
    # Source: https://www.mof.gov.tw/english/
    return pd.DataFrame([
        {"month": "Jan 2024", "exports_usd_bn": 7.2,  "yoy_pct": 18.3},
        {"month": "Feb 2024", "exports_usd_bn": 6.1,  "yoy_pct": 22.1},
        {"month": "Mar 2024", "exports_usd_bn": 8.4,  "yoy_pct": 31.2},
        {"month": "Apr 2024", "exports_usd_bn": 8.9,  "yoy_pct": 35.6},
        {"month": "May 2024", "exports_usd_bn": 9.2,  "yoy_pct": 38.4},
        {"month": "Jun 2024", "exports_usd_bn": 9.8,  "yoy_pct": 40.1},
        {"month": "Jul 2024", "exports_usd_bn": 10.3, "yoy_pct": 42.3},
        {"month": "Aug 2024", "exports_usd_bn": 11.1, "yoy_pct": 45.7},
        {"month": "Sep 2024", "exports_usd_bn": 10.7, "yoy_pct": 41.2},
        {"month": "Oct 2024", "exports_usd_bn": 11.5, "yoy_pct": 44.8},
        {"month": "Nov 2024", "exports_usd_bn": 12.2, "yoy_pct": 48.3},
        {"month": "Dec 2024", "exports_usd_bn": 11.8, "yoy_pct": 43.1},
        {"month": "Jan 2025", "exports_usd_bn": 13.1, "yoy_pct": 81.9},
        {"month": "Feb 2025", "exports_usd_bn": 11.4, "yoy_pct": 86.9},
    ])


@st.cache_data(ttl=900)
def fetch_infra_proxy_stocks(tickers: tuple) -> pd.DataFrame:
    """Fetch stock performance for AI infrastructure proxy stocks."""
    rows = []
    today = datetime.today()
    for ticker, (name, desc) in zip(tickers, [INFRA_PROXIES[t] for t in tickers]):
        try:
            t_obj = yf.Ticker(ticker)
            h     = t_obj.history(period="370d")
            if len(h) < 2:
                continue
            price  = h["Close"].iloc[-1]
            d1     = round(((h["Close"].iloc[-1] / h["Close"].iloc[-2]) - 1) * 100, 2)
            ytd_h  = t_obj.history(start=datetime(today.year,1,1).strftime("%Y-%m-%d"))
            ytd    = round(((ytd_h["Close"].iloc[-1]/ytd_h["Close"].iloc[0])-1)*100,2) if len(ytd_h)>1 else None
            h1y_s  = h["Close"].iloc[0] if len(h) >= 250 else None
            y1     = round(((price / h1y_s) - 1) * 100, 2) if h1y_s else None
            # Get quarterly revenue from financials
            try:
                fin = t_obj.quarterly_financials
                rev_row = fin.loc["Total Revenue"] if "Total Revenue" in fin.index else None
                latest_rev = float(rev_row.iloc[0]) / 1e9 if rev_row is not None else None
                prev_rev   = float(rev_row.iloc[1]) / 1e9 if rev_row is not None and len(rev_row) > 1 else None
                rev_qoq    = round(((latest_rev/prev_rev)-1)*100,1) if (latest_rev and prev_rev) else None
            except Exception:
                latest_rev, rev_qoq = None, None

            rows.append({
                "Ticker":   ticker,
                "Company":  name,
                "Role":     desc,
                "Price":    round(price, 2),
                "1D %":     d1,
                "YTD %":    ytd,
                "1Y %":     y1,
                "Rev (Q, $B)": round(latest_rev, 2) if latest_rev else None,
                "Rev QoQ":  rev_qoq,
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600)
def fetch_mag7_live_capex(tickers: tuple) -> dict:
    """
    Supplement hardcoded capex with live quarterly cashflow from yfinance.
    Returns dict: ticker → latest quarterly capex in $B
    """
    live = {}
    for ticker in tickers:
        try:
            t_obj = yf.Ticker(ticker)
            cf    = t_obj.quarterly_cashflow
            if cf is None or cf.empty:
                continue
            # Capital expenditures row — yfinance uses negative sign for outflows
            capex_row = None
            for label in ["Capital Expenditure", "Purchase Of Property Plant And Equipment",
                           "Capital Expenditures", "Purchases of property and equipment"]:
                if label in cf.index:
                    capex_row = cf.loc[label]
                    break
            if capex_row is not None:
                val = float(capex_row.iloc[0])
                live[ticker] = round(abs(val) / 1e9, 2)
        except Exception:
            continue
    return live


# ── Existing helpers ──────────────────────────────────────────────────────────
def fmt_value(v):
    if v >= 1_000_000_000: return f"{v/1_000_000_000:.1f}B"
    if v >= 1_000_000:     return f"{v/1_000_000:.1f}M"
    if v >= 1_000:         return f"{v/1_000:.1f}K"
    return str(int(v))

def color_pct(val):
    if isinstance(val, (int, float)):
        return "color: #3fb950" if val >= 0 else "color: #f85149"
    return ""

def time_ago(pub_date_str):
    try:
        from email.utils import parsedate_to_datetime
        dt   = parsedate_to_datetime(pub_date_str)
        diff = datetime.now(dt.tzinfo) - dt
        if diff.seconds < 3600: return f"{diff.seconds//60}m ago"
        if diff.days == 0:      return f"{diff.seconds//3600}h ago"
        return f"{diff.days}d ago"
    except:
        return ""

def tag_html(text, color="#58a6ff", bg="rgba(0,128,255,0.12)"):
    return f'<span style="display:inline-block;background:{bg};color:{color};border-radius:4px;padding:1px 7px;font-size:10px;margin-right:4px">{text}</span>'

@st.cache_data(ttl=900)
def get_index_performance(tickers: list):
    today = datetime.today()
    for ticker in tickers:
        try:
            t  = yf.Ticker(ticker)
            h2 = t.history(period="2d")
            if len(h2) < 2: continue
            h52 = t.history(period="1y")
            w52_high = h52["High"].max()  if len(h52) > 0 else None
            w52_low  = h52["Low"].min()   if len(h52) > 0 else None
            h5y = t.history(period="5y")
            ath = h5y["High"].max() if len(h5y) > 0 else None
            def pct(days):
                h = t.history(start=(today - timedelta(days=days)).strftime("%Y-%m-%d"))
                if len(h) < 2: return None
                return round(((h["Close"].iloc[-1] / h["Close"].iloc[0]) - 1) * 100, 2)
            price = h2["Close"].iloc[-1]
            d1    = round(((h2["Close"].iloc[-1] / h2["Close"].iloc[-2]) - 1) * 100, 2)
            ytd_h = t.history(start=datetime(today.year, 1, 1).strftime("%Y-%m-%d"))
            ytd   = round(((ytd_h["Close"].iloc[-1] / ytd_h["Close"].iloc[0]) - 1) * 100, 2) if len(ytd_h) > 1 else None
            pct_from_52w = round(((price / w52_high) - 1) * 100, 2) if w52_high else None
            pct_from_ath = round(((price / ath) - 1) * 100, 2)      if ath      else None
            is_ath       = (pct_from_ath is not None and abs(pct_from_ath) < 0.5)
            return {"price": price, "1D": d1, "30D": pct(30), "YTD": ytd,
                    "3Y": pct(365*3), "5Y": pct(365*5), "source_ticker": ticker,
                    "52w_high": w52_high, "52w_low": w52_low,
                    "pct_from_52w": pct_from_52w,
                    "ath": ath, "pct_from_ath": pct_from_ath, "is_ath": is_ath}
        except: continue
    return None

@st.cache_data(ttl=900)
def get_commodity_performance(tickers: list):
    return get_index_performance(tickers)

@st.cache_data(ttl=900)
def get_sector_etf_perf(etf_map: tuple):
    rows = []
    for sector, ticker in etf_map:
        try:
            t     = yf.Ticker(ticker)
            today = datetime.today()
            h2    = t.history(period="2d")
            if len(h2) < 2: continue
            price = h2["Close"].iloc[-1]
            d1    = round(((h2["Close"].iloc[-1] / h2["Close"].iloc[-2]) - 1) * 100, 2)
            def pct(days):
                h = t.history(start=(today - timedelta(days=days)).strftime("%Y-%m-%d"))
                if len(h) < 2: return None
                return round(((h["Close"].iloc[-1] / h["Close"].iloc[0]) - 1) * 100, 2)
            ytd_h = t.history(start=datetime(today.year, 1, 1).strftime("%Y-%m-%d"))
            ytd   = round(((ytd_h["Close"].iloc[-1] / ytd_h["Close"].iloc[0]) - 1) * 100, 2) if len(ytd_h) > 1 else None
            rows.append({"Sector": sector, "ETF": ticker, "Price": round(price,2),
                         "1D": d1, "1M": pct(30), "YTD": ytd, "1Y": pct(365), "3Y": pct(365*3)})
        except: continue
    return pd.DataFrame(rows).set_index("Sector") if rows else pd.DataFrame()

@st.cache_data(ttl=900)
def get_stocks_data(stock_dict_items: tuple, history_arg: str, period_label: str):
    rows = []
    for ticker, (name, sector, sub) in stock_dict_items:
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(period=history_arg)
            if len(hist) < 2: continue
            price     = hist["Close"].iloc[-1]
            prev      = hist["Close"].iloc[-2] if period_label == "1D" else hist["Close"].iloc[0]
            change    = round(((price / prev) - 1) * 100, 2)
            vol_today = hist["Volume"].iloc[-1]
            value     = price * vol_today
            rows.append({
                "Ticker":                   ticker.split(".")[0],
                "Company":                  name,
                "Sector":                   sector,
                "Sub-Sector":               sub,
                "Price":                    round(price, 2),
                f"Change ({period_label})": change,
                "Volume":                   int(vol_today),
                "Value Traded":             value,
            })
        except: continue
    return pd.DataFrame(rows)

@st.cache_data(ttl=1800)
def fetch_news(query: str, max_items: int = 12):
    try:
        q   = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            xml = r.read().decode("utf-8")
        items   = re.findall(r"<item>(.*?)</item>", xml, re.DOTALL)
        results = []
        for item in items[:max_items]:
            title   = re.search(r"<title>(.*?)</title>", item)
            link    = re.search(r"<link/>(.*?)\n", item) or re.search(r"<link>(.*?)</link>", item)
            pubdate = re.search(r"<pubDate>(.*?)</pubDate>", item)
            desc    = re.search(r"<description>(.*?)</description>", item, re.DOTALL)
            title_t = re.sub(r"<[^>]+>","", title.group(1)).strip() if title else ""
            if " - " in title_t:
                title_t, src_t = title_t.rsplit(" - ",1)[0].strip(), title_t.rsplit(" - ",1)[-1].strip()
            else:
                src_t = ""
            if title_t:
                results.append({
                    "title":  title_t,
                    "source": src_t,
                    "link":   (link.group(1) if link else "").strip(),
                    "age":    time_ago((pubdate.group(1) if pubdate else "").strip()),
                    "desc":   re.sub(r"<[^>]+>","", desc.group(1) if desc else "").strip()[:180],
                })
        return results
    except: return []


# ═════════════════════════════════════════════════════════════════════════════
# APP LAYOUT
# ═════════════════════════════════════════════════════════════════════════════
st.title("📈 Global Market Monitor")
st.caption(f"Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ·  Yahoo Finance + Google News  ·  Cache 15 min")

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=15 * 60 * 1000, key="autorefresh")
except ImportError:
    pass

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌐 Indices", "🏭 US Sectors", "🔍 Stock Screener", "📰 News", "🤖 AI Capex Tracker"
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — INDICES
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Global Index Performance")
    with st.spinner("Loading indices..."):
        idx_rows = []
        prog = st.progress(0)
        for i, (name, meta) in enumerate(INDICES.items()):
            d = get_index_performance(meta["tickers"])
            prog.progress((i+1)/len(INDICES))
            if d:
                idx_rows.append({"Index": name, "Price": d["price"],
                                 "1D": d["1D"], "30D": d["30D"], "YTD": d["YTD"],
                                 "3Y": d["3Y"], "5Y": d["5Y"],
                                 "source_ticker": d.get("source_ticker", ""),
                                 "52w_high": d.get("52w_high"),
                                 "pct_from_52w": d.get("pct_from_52w"),
                                 "ath": d.get("ath"),
                                 "pct_from_ath": d.get("pct_from_ath"),
                                 "is_ath": d.get("is_ath", False)})
        prog.empty()
    failed = [n for n in INDICES if n not in [r["Index"] for r in idx_rows]]
    if failed:
        st.warning(f"⚠️ Could not load: {', '.join(failed)}")

    if idx_rows:
        df_idx = pd.DataFrame(idx_rows).set_index("Index")
        cols = st.columns(5)
        for i, (name, row) in enumerate(df_idx.iterrows()):
            with cols[i % 5]:
                src = f" ({row.get('source_ticker','')})" if row.get("source_ticker","").startswith("EW") else ""
                st.metric(name + src, f"{row['Price']:,.2f}",
                          delta=f"{row['1D']:+.2f}%" if row["1D"] is not None else "—")
        st.write("")

        def fmt_52w(row):
            if row.get("is_ath"): return "🏆 ATH"
            v = row.get("pct_from_52w")
            if v is None: return "—"
            if v >= -1:   return f"🔝 Near 52W High ({v:+.1f}%)"
            return f"{v:+.1f}% from 52W High"

        def fmt_ath(row):
            v = row.get("pct_from_ath")
            if v is None: return "—"
            if abs(v) < 0.5: return "🏆 At ATH"
            return f"{v:+.1f}% from ATH"

        perf_df = df_idx[["1D","30D","YTD","3Y","5Y"]].copy()
        perf_df["vs 52W High"] = [fmt_52w(r) for _, r in df_idx.iterrows()]
        perf_df["vs ATH (5Y)"] = [fmt_ath(r) for _, r in df_idx.iterrows()]

        styled_idx = (perf_df.style
                      .applymap(color_pct, subset=["1D","30D","YTD","3Y","5Y"])
                      .format({c: (lambda x: f"{x:+.2f}%" if pd.notna(x) else "—")
                               for c in ["1D","30D","YTD","3Y","5Y"]}))
        st.dataframe(styled_idx, use_container_width=True, height=390)
        st.divider()

        st.subheader("🛢️ Commodities")
        comm_rows = []
        comm_prog = st.progress(0)
        for i, (cname, cmeta) in enumerate(COMMODITIES.items()):
            cd = get_commodity_performance(cmeta["tickers"])
            comm_prog.progress((i+1)/len(COMMODITIES))
            if cd:
                comm_rows.append({"Commodity": cname, "Unit": cmeta["unit"],
                                  "Price": cd["price"], "1D": cd["1D"],
                                  "30D": cd["30D"], "YTD": cd["YTD"],
                                  "pct_from_52w": cd.get("pct_from_52w"),
                                  "is_ath": cd.get("is_ath", False),
                                  "pct_from_ath": cd.get("pct_from_ath")})
        comm_prog.empty()

        if comm_rows:
            ccols = st.columns(len(comm_rows))
            for i, crow in enumerate(comm_rows):
                with ccols[i]:
                    price_str = f"{crow['Price']:,.2f}" if crow["Price"] < 10000 else f"{crow['Price']:,.0f}"
                    st.metric(crow["Commodity"], f"{price_str} {crow['Unit'].split('/')[0]}",
                              delta=f"{crow['1D']:+.2f}%" if crow["1D"] is not None else "—")
            st.write("")
            cdf = pd.DataFrame(comm_rows).set_index("Commodity")
            disp_c = cdf[["Unit","Price","1D","30D","YTD"]].copy()
            disp_c["Price"] = disp_c["Price"].apply(lambda x: f"{x:,.2f}")
            styled_c = (disp_c.style
                        .applymap(color_pct, subset=["1D","30D","YTD"])
                        .format({c: (lambda x: f"{x:+.2f}%" if pd.notna(x) else "—")
                                 for c in ["1D","30D","YTD"]}))
            st.dataframe(styled_c, use_container_width=True, height=230)
    else:
        st.error("Could not load any index data.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — US SECTOR ETFs
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("US Sector Returns  ·  SPDR Sector ETFs")
    etf_items = tuple(SECTOR_ETFS.items())
    with st.spinner("Loading sector ETFs..."):
        df_sec = get_sector_etf_perf(etf_items)
    if not df_sec.empty:
        st.markdown("**YTD Performance by Sector**")
        chart_df = df_sec[["YTD"]].dropna().sort_values("YTD", ascending=True)
        st.bar_chart(chart_df)
        st.markdown("**Full Sector Table**")
        disp_cols = ["ETF","Price","1D","1M","YTD","1Y","3Y"]
        styled_sec = (df_sec[disp_cols].style
                      .applymap(color_pct, subset=["1D","1M","YTD","1Y","3Y"])
                      .format({"Price": "{:,.2f}",
                               "1D":  lambda x: f"{x:+.2f}%" if pd.notna(x) else "—",
                               "1M":  lambda x: f"{x:+.2f}%" if pd.notna(x) else "—",
                               "YTD": lambda x: f"{x:+.2f}%" if pd.notna(x) else "—",
                               "1Y":  lambda x: f"{x:+.2f}%" if pd.notna(x) else "—",
                               "3Y":  lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"}))
        st.dataframe(styled_sec, use_container_width=True, height=430)
        ytd_valid = df_sec["YTD"].dropna()
        if not ytd_valid.empty:
            best_s, worst_s = ytd_valid.idxmax(), ytd_valid.idxmin()
            c1,c2,c3 = st.columns(3)
            c1.metric("🏆 Best YTD",  best_s,  f"{ytd_valid[best_s]:+.2f}%")
            c2.metric("📉 Worst YTD", worst_s, f"{ytd_valid[worst_s]:+.2f}%")
            c3.metric("📊 Avg YTD",   "All sectors", f"{ytd_valid.mean():+.2f}%")
    else:
        st.error("Could not load sector ETF data.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — STOCK SCREENER
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Stock Screener")
    c1,c2,c3,c4,c5 = st.columns([2,2,2,2,1])
    with c1: market_choice = st.selectbox("Market",    list(MARKET_STOCK_MAP.keys()))
    with c2: timeframe     = st.selectbox("Timeframe", list(PERIOD_MAP.keys()))
    with c3: screen_mode   = st.selectbox("Screen by", [
                "🔥 Top Active (Value)","🚀 Top Gainers","📉 Top Losers","📊 All (by Change)"])
    with c4:
        stock_dict    = MARKET_STOCK_MAP[market_choice]
        avail_sectors = ["All Sectors"] + sorted(set(v[1] for v in stock_dict.values()))
        sector_filter = st.selectbox("Sector", avail_sectors)
    with c5: top_n = st.selectbox("Show", [10, 20, 30])

    history_arg, period_label = PERIOD_MAP[timeframe]
    change_col = f"Change ({period_label})"
    stock_items = tuple(stock_dict.items())

    with st.spinner(f"Loading {market_choice}..."):
        df_all = get_stocks_data(stock_items, history_arg, period_label)

    if df_all.empty:
        st.warning("No data returned.")
    else:
        df_filtered = df_all[df_all["Sector"] == sector_filter].copy() if sector_filter != "All Sectors" else df_all.copy()
        if screen_mode == "🔥 Top Active (Value)":
            df_out = df_filtered.nlargest(top_n, "Value Traded")
        elif screen_mode == "🚀 Top Gainers":
            df_out = df_filtered.nlargest(top_n, change_col)
        elif screen_mode == "📉 Top Losers":
            df_out = df_filtered.nsmallest(top_n, change_col)
        else:
            df_out = df_filtered.sort_values(change_col, ascending=False).head(top_n)
        df_out = df_out.reset_index(drop=True)
        df_out.index += 1; df_out.index.name = "Rank"
        if not df_out.empty:
            gainers = (df_out[change_col] > 0).sum()
            losers  = (df_out[change_col] < 0).sum()
            m1,m2,m3,m4,m5 = st.columns(5)
            m1.metric("Pool size", f"{len(df_filtered)} stocks")
            m2.metric("Showing",   len(df_out))
            m3.metric("🟢 Gainers", gainers)
            m4.metric("🔴 Losers",  losers)
            m5.metric(f"Avg {period_label}", f"{df_out[change_col].mean():+.2f}%")
        display = df_out.copy()
        display["Value Traded"] = display["Value Traded"].apply(fmt_value)
        display["Volume"]  = display["Volume"].apply(fmt_value)
        display["Price"]   = display["Price"].apply(lambda x: f"{x:,.2f}")
        styled = (display.style
                  .applymap(color_pct, subset=[change_col])
                  .format({change_col: lambda x: f"{x:+.2f}%" if isinstance(x,(int,float)) else x}))
        st.dataframe(styled, use_container_width=True, height=min(120+len(df_out)*36, 650))
        csv = df_out.to_csv()
        st.download_button("⬇️ Download CSV", data=csv,
            file_name=f"{market_choice.split()[1]}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — NEWS
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Market News Feed")
    nc1,nc2 = st.columns([3,1])
    with nc1: news_cat   = st.selectbox("Category", list(NEWS_CATEGORIES.keys()))
    with nc2: news_count = st.selectbox("Articles", [10,15,20])
    custom_q = st.text_input("Search specific topic / stock", placeholder="e.g. NVIDIA earnings, Fed rate...")
    query    = custom_q.strip() if custom_q.strip() else NEWS_CATEGORIES[news_cat]
    with st.spinner("Fetching news..."):
        articles = fetch_news(query, max_items=news_count)
    if not articles:
        st.warning("Could not fetch news.")
    else:
        st.caption(f"{len(articles)} articles · **{query}**")
        cat_colors = {
            "💰 Finance & Economy":    ("#ffd700","rgba(255,215,0,0.1)"),
            "⚔️ Geopolitics & War":    ("#f85149","rgba(248,81,73,0.1)"),
            "🏛️ Politics & Policy":    ("#d29922","rgba(210,153,34,0.1)"),
            "💻 Technology & AI":      ("#58a6ff","rgba(0,128,255,0.12)"),
            "🛢️ Energy & Commodities": ("#3fb950","rgba(63,185,80,0.1)"),
            "🏦 Banking & Crypto":     ("#bc8cff","rgba(188,140,255,0.1)"),
            "🏭 Industry & Trade":     ("#ff7b72","rgba(255,123,114,0.1)"),
            "🌍 All Markets":          ("#00d4aa","rgba(0,212,170,0.1)"),
        }
        tc, bg = cat_colors.get(news_cat, ("#58a6ff","rgba(0,128,255,0.12)"))
        cat_short = news_cat.split(" ",1)[1] if " " in news_cat else news_cat
        col_a, col_b = st.columns(2)
        for i, art in enumerate(articles):
            col = col_a if i % 2 == 0 else col_b
            with col:
                age_str  = f" · {art['age']}" if art['age'] else ""
                link_s   = f'<a href="{art["link"]}" target="_blank" style="text-decoration:none;color:inherit">' if art['link'] else ""
                link_e   = "</a>" if art['link'] else ""
                desc_snip= f"<div style='font-size:12px;color:#7d8590;margin:5px 0'>{art['desc'][:150]}...</div>" if art['desc'] else ""
                st.markdown(f"""
                <div class="news-card">
                  <div class="news-title">{link_s}{art['title']}{link_e}</div>
                  {desc_snip}
                  <div class="news-meta">{tag_html(cat_short,tc,bg)}<span style="color:#7d8590">{art['source']}{age_str}</span></div>
                </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — AI CAPEX TRACKER
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("🤖 AI Capex Tracker")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}  ·  Sources: 10-Q/10-K · ERCOT · ArXiv · Taiwan MoF · Yahoo Finance · company press releases")

    # ══════════════════════════════════════════════════════════════════════════
    # PRE-FETCH ALL DATA (cached)
    # ══════════════════════════════════════════════════════════════════════════
    ercot = fetch_ercot_load_queue()
    tw_df = fetch_taiwan_exports()

    mag7_latest_total = sum(v["quarters"][0][2] for v in MAG7_CAPEX_HISTORY.values() if v["quarters"])
    mag7_prev_total   = sum(v["quarters"][1][2] for v in MAG7_CAPEX_HISTORY.values() if len(v["quarters"]) > 1)
    mag7_qoq          = round(((mag7_latest_total / mag7_prev_total) - 1) * 100, 1) if mag7_prev_total else 0

    tw_yoy_latest = tw_df.iloc[-1]["yoy_pct"] if not tw_df.empty else 0
    tw_trend      = round(((tw_df["exports_usd_bn"].iloc[-3:].mean() /
                            tw_df["exports_usd_bn"].iloc[-6:-3].mean()) - 1) * 100, 1) if len(tw_df) >= 6 else 0

    try:
        _ax = fetch_arxiv_ai_volume(weeks=8)
        _ax_clean = _ax.dropna(subset=["papers"])
        if len(_ax_clean) >= 4:
            ax_recent = _ax_clean["papers"].iloc[-2:].mean()
            ax_prior  = _ax_clean["papers"].iloc[-6:-2].mean()
            ax_chg    = round(((ax_recent / ax_prior) - 1) * 100, 1) if ax_prior else 0
        else:
            ax_chg = 0
    except Exception:
        ax_chg = 0

    try:
        _px = fetch_infra_proxy_stocks(("SMCI", "ANET", "VRT"))
        proxy_ytd_avg = _px["YTD %"].dropna().mean() if not _px.empty else 0
    except Exception:
        proxy_ytd_avg = 0

    def score_signal(val, green_thresh, red_thresh, higher_is_better=True):
        if higher_is_better:
            return 1 if val >= green_thresh else (-1 if val <= red_thresh else 0)
        else:
            return 1 if val <= green_thresh else (-1 if val >= red_thresh else 0)

    s_mag7   = score_signal(mag7_qoq,       5,  -5)
    s_ercot  = score_signal(ercot.get("yoy_change_pct", 20), 20, 0)
    s_taiwan = score_signal(tw_yoy_latest,  30,  10)
    s_arxiv  = score_signal(ax_chg,          5, -10)
    s_proxy  = score_signal(proxy_ytd_avg,  10, -10)

    VRT_BACKLOG = [
        ("Q4'25","Oct–Dec 2025",15.0,2.9,252),
        ("Q3'25","Jul–Sep 2025", 9.5,1.4, 60),
        ("Q2'25","Apr–Jun 2025", 8.5,1.2, 15),
        ("Q1'25","Jan–Mar 2025", 7.8,1.1, 10),
        ("Q4'24","Oct–Dec 2024", 7.2,1.3, 18),
        ("Q3'24","Jul–Sep 2024", 6.6,1.2, 55),
        ("Q2'24","Apr–Jun 2024", 5.8,1.1, 57),
        ("Q1'24","Jan–Mar 2024", 5.1,1.0, 60),
    ]
    vrt_latest_btb = VRT_BACKLOG[0][3]
    s_vrt = score_signal(vrt_latest_btb, 1.2, 0.9)

    # Construction basket scores (hardcoded latest)
    PWR_BACKLOG_BN = 39.2   # Q3'25
    POWL_LEAD_WKS  = 80     # avg 80-100wk
    s_construction = score_signal(PWR_BACKLOG_BN, 35, 25)

    all_scores = [s_mag7, s_ercot, s_taiwan, s_arxiv, s_proxy, s_vrt, s_construction]
    n_green = sum(1 for s in all_scores if s ==  1)
    n_red   = sum(1 for s in all_scores if s == -1)
    total_score = sum(all_scores)

    if total_score >= 5:
        verdict_emoji, verdict_label = "🟢", "EXPANSION"
        verdict_color = "#3fb950"
        verdict_desc  = "AI capex cycle firmly in expansion. All major signals pointing up across demand, build, chip, and power layers."
    elif total_score >= 2:
        verdict_emoji, verdict_label = "🟡", "CAUTIOUS"
        verdict_color = "#d29922"
        verdict_desc  = "Mixed signals. Leading indicators softening but no confirmed reversal. Watch for a second consecutive weak quarter."
    elif total_score >= -1:
        verdict_emoji, verdict_label = "🟠", "EARLY WARNING"
        verdict_color = "#e3892b"
        verdict_desc  = "Multiple leading signals turning negative. High-conviction slowdown not yet confirmed but risk is elevated."
    else:
        verdict_emoji, verdict_label = "🔴", "SLOWDOWN"
        verdict_color = "#f85149"
        verdict_desc  = "Broad-based deterioration. AI capex slowdown likely underway. Lagging indicators will confirm within 1–2 quarters."

    # ══════════════════════════════════════════════════════════════════════════
    # COMPOSITE VERDICT BOX
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#0d1117 0%,#111820 100%);
     border:2px solid {verdict_color};border-radius:12px;padding:24px 28px;margin-bottom:20px">
  <div style="display:flex;align-items:center;gap:14px;margin-bottom:12px">
    <div style="font-size:48px;line-height:1">{verdict_emoji}</div>
    <div>
      <div style="font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:#7d8590;font-family:monospace;margin-bottom:4px">
        AI CAPEX CYCLE · COMPOSITE VERDICT · {n_green}/{len(all_scores)} SIGNALS GREEN
      </div>
      <div style="font-size:32px;font-weight:800;font-family:monospace;color:{verdict_color};letter-spacing:0.05em">{verdict_label}</div>
    </div>
    <div style="margin-left:auto;text-align:right">
      <div style="font-size:11px;color:#7d8590;font-family:monospace">SIGNAL SCORE</div>
      <div style="font-size:36px;font-weight:700;color:{verdict_color};font-family:monospace">{total_score:+d}</div>
      <div style="font-size:10px;color:#7d8590">out of +{len(all_scores)}</div>
    </div>
  </div>
  <div style="font-size:14px;color:#c9d1d9;line-height:1.6;border-top:1px solid #1c2333;padding-top:12px">{verdict_desc}</div>
</div>
""", unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SUPPLY CHAIN CASCADE — 4 LAYERS, EACH WITH STATUS CARDS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("""
<div style="background:#0d1117;border:1px solid #1c2333;border-radius:8px;padding:14px 18px;margin-bottom:20px">
  <div style="font-size:11px;letter-spacing:0.12em;text-transform:uppercase;color:#7d8590;font-family:monospace;margin-bottom:10px">
    📐 HOW TO READ THIS PAGE — SUPPLY CHAIN CASCADE LOGIC
  </div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;font-family:monospace;font-size:11px">
    <div style="text-align:center;padding:8px;background:#111820;border-radius:6px;border-top:2px solid #4285f4">
      <div style="font-size:16px">🧠</div>
      <div style="color:#4285f4;font-weight:700;margin:4px 0">LAYER 1</div>
      <div style="color:#e6edf3">DEMAND SIGNALS</div>
      <div style="color:#7d8590;font-size:10px;margin-top:4px">ArXiv research volume<br>Taiwan→US exports<br><i>Leads by 1–2Q</i></div>
    </div>
    <div style="text-align:center;padding:8px;background:#111820;border-radius:6px;border-top:2px solid #f0883e">
      <div style="font-size:16px">🏗️</div>
      <div style="color:#f0883e;font-weight:700;margin:4px 0">LAYER 2</div>
      <div style="color:#e6edf3">BUILD SIGNALS</div>
      <div style="color:#7d8590;font-size:10px;margin-top:4px">ERCOT power queue<br>Nuclear PPAs<br>Construction backlog<br><i>Leads by 0–1Q</i></div>
    </div>
    <div style="text-align:center;padding:8px;background:#111820;border-radius:6px;border-top:2px solid #a371f7">
      <div style="font-size:16px">⚙️</div>
      <div style="color:#a371f7;font-weight:700;margin:4px 0">LAYER 3</div>
      <div style="color:#e6edf3">EQUIPMENT SIGNALS</div>
      <div style="color:#7d8590;font-size:10px;margin-top:4px">Vertiv backlog & BTB<br>WFE basket revenue<br><i>Coincident</i></div>
    </div>
    <div style="text-align:center;padding:8px;background:#111820;border-radius:6px;border-top:2px solid #f85149">
      <div style="font-size:16px">📊</div>
      <div style="color:#f85149;font-weight:700;margin:4px 0">LAYER 4</div>
      <div style="color:#e6edf3">CONFIRMATION</div>
      <div style="color:#7d8590;font-size:10px;margin-top:4px">Mag7 reported capex<br>Arista revenue<br><i>Lags by 1–2Q</i></div>
    </div>
  </div>
  <div style="margin-top:10px;font-size:11px;color:#7d8590;font-family:monospace">
    ⚡ Read left to right: Layer 1 turns red first → expect Layer 4 to follow in 2–3 quarters.
    All 4 layers red simultaneously = high-conviction slowdown confirmed.
  </div>
</div>
""", unsafe_allow_html=True)

    # ── LAYER STATUS INDICATORS ────────────────────────────────────────────────
    def layer_card(layer_num, emoji, label, color, signals, summary):
        dots = "".join(
            f'<span style="color:{"#3fb950" if s==1 else "#f85149" if s==-1 else "#d29922"};font-size:16px">●</span> '
            for s in signals
        )
        layer_score = sum(signals)
        layer_max   = len(signals)
        if layer_score == layer_max:   bg, status = "rgba(63,185,80,0.08)",  "ALL GREEN"
        elif layer_score <= -layer_max: bg, status = "rgba(248,81,73,0.08)",  "ALL RED"
        elif layer_score > 0:          bg, status = "rgba(63,185,80,0.04)",   "MOSTLY GREEN"
        elif layer_score < 0:          bg, status = "rgba(248,81,73,0.04)",   "MOSTLY RED"
        else:                          bg, status = "rgba(210,153,34,0.06)",   "MIXED"
        return f"""<div style="background:{bg};border:1px solid {color}33;border-top:3px solid {color};
            border-radius:8px;padding:14px;height:100%">
          <div style="font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:{color};font-family:monospace;margin-bottom:6px">
            {emoji} LAYER {layer_num} · {label}
          </div>
          <div style="margin-bottom:8px">{dots}</div>
          <div style="font-size:13px;font-weight:600;font-family:monospace;color:#e6edf3;margin-bottom:4px">{status}</div>
          <div style="font-size:11px;color:#7d8590;line-height:1.5">{summary}</div>
        </div>"""

    l1, l2, l3, l4 = st.columns(4)
    tw_s   = score_signal(tw_yoy_latest, 30, 10)
    ax_s   = score_signal(ax_chg, 5, -10)
    er_s   = score_signal(ercot.get("yoy_change_pct", 20), 20, 0)
    nuc_s  = 1  # always green — PPAs are legally binding and already signed
    con_s  = score_signal(PWR_BACKLOG_BN, 35, 25)
    vrt_s  = score_signal(vrt_latest_btb, 1.2, 0.9)
    wfe_s  = 0  # placeholder — populated after live fetch below
    ar_s   = score_signal(ax_chg, 5, -10)
    mag_s  = score_signal(mag7_qoq, 5, -5)

    with l1:
        st.markdown(layer_card(1,"🧠","DEMAND",    "#4285f4", [tw_s, ax_s],
            f"Taiwan YoY: {tw_yoy_latest:+.0f}%<br>ArXiv chg: {ax_chg:+.0f}%"), unsafe_allow_html=True)
    with l2:
        st.markdown(layer_card(2,"🏗️","BUILD",      "#f0883e", [er_s, nuc_s, con_s],
            f"ERCOT: {ercot.get('large_load_gw','—')} GW queued<br>Nuclear PPAs: 16.2 GW committed<br>PWR backlog: ${PWR_BACKLOG_BN:.0f}B"), unsafe_allow_html=True)
    with l3:
        st.markdown(layer_card(3,"⚙️","EQUIPMENT", "#a371f7", [vrt_s],
            f"Vertiv BTB: {vrt_latest_btb:.1f}x<br>WFE basket: see below"), unsafe_allow_html=True)
    with l4:
        st.markdown(layer_card(4,"📊","CONFIRMATION","#f85149", [mag_s],
            f"Mag7 capex QoQ: {mag7_qoq:+.1f}%<br>Arista rev: see below"), unsafe_allow_html=True)

    st.markdown("")

    # Reading guide in expander
    with st.expander("📖 How to read these signals — click to expand", expanded=False):
        st.markdown("""
#### Supply Chain Cascade Logic

| Layer | Signals | Lead Time | Interpretation |
|---|---|---|---|
| 🧠 **1 — Demand** | Taiwan exports, ArXiv papers | **1–2Q ahead** | Orders placed before hardware ships or researchers hired |
| 🏗️ **2 — Build** | ERCOT queue, Nuclear PPAs, Construction backlog | **0–1Q ahead** | Committed infrastructure — legally binding, hard to cancel |
| ⚙️ **3 — Equipment** | Vertiv BTB, WFE basket, Arista revenue | **Coincident** | Equipment ships when data centers are being fitted out |
| 📊 **4 — Confirmation** | Mag7 reported capex | **1–2Q lagging** | Backward-looking — confirms what already happened |

**Reading the cascade:**
- 🟢🟢🟢🟢 All layers green → Full expansion, cycle intact
- 🔴⬜⬜⬜ Only Layer 1 red → Early warning, wait for Layer 2 to confirm
- 🔴🔴⬜⬜ Layers 1+2 red → High conviction — reduce AI infra exposure
- 🔴🔴🔴🟢 Layers 1–3 red, Layer 4 still green → Lagging data masking slowdown already underway

**Score interpretation:** Each signal scores +1 (green), 0 (yellow), or −1 (red).
Total score ≥ +5 = expansion · +2 to +4 = cautious · −1 to +1 = early warning · ≤ −2 = slowdown
        """)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # LAYER 1 — DEMAND SIGNALS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("## 🧠 Layer 1 — Demand Signals")
    st.caption("These move first. A sustained drop here signals spending cuts 1–2 quarters before earnings reflect it.")

    demand_tab1, demand_tab2 = st.tabs(["🚢 Taiwan → US Exports", "📄 ArXiv AI Research Volume"])

    with demand_tab1:
        st.markdown("""
**What it is:** Monthly value of goods Taiwan ships to the US. Taiwan makes the world's most advanced GPUs (TSMC),
HBM memory stacks, and server motherboards. GPU orders placed by US hyperscalers show up here ~1 quarter before
they appear in any capex filing.

**What to watch:** YoY > 30% = accelerating. 3M trend vs prior 3M = cleanest direction signal.
Deceleration from 40%+ to below 15% over 2 consecutive months = serious warning.
⚠️ Jan/Feb 2025 spike partly reflects tariff front-running — not pure AI demand.
""")
        if not tw_df.empty:
            tw_display = tw_df.copy().set_index("month")
            tw_c1, tw_c2 = st.columns([3, 1])
            with tw_c1:
                import plotly.graph_objects as go
                yoy_colors = ["#3fb950" if v >= 30 else ("#d29922" if v >= 10 else "#f85149")
                              for v in tw_display["yoy_pct"]]
                fig_tw = go.Figure()
                fig_tw.add_trace(go.Bar(
                    x=tw_display.index, y=tw_display["exports_usd_bn"],
                    name="Exports $B", yaxis="y1",
                    marker_color=["#3fb950" if v >= tw_display["exports_usd_bn"].mean() else "#1c2333"
                                  for v in tw_display["exports_usd_bn"]],
                    hovertemplate="<b>%{x}</b><br>Exports: $%{y:.1f}B<extra></extra>"
                ))
                fig_tw.add_trace(go.Scatter(
                    x=tw_display.index, y=tw_display["yoy_pct"],
                    name="YoY %", yaxis="y2", mode="lines+markers",
                    line=dict(color="#ff9900", width=2.5),
                    marker=dict(size=7, color=yoy_colors, line=dict(color="#0d1117", width=1)),
                    hovertemplate="<b>%{x}</b><br>YoY: %{y:+.1f}%<extra></extra>"
                ))
                fig_tw.add_hline(y=tw_display["exports_usd_bn"].mean(), yref="y",
                                 line_dash="dot", line_color="#58a6ff",
                                 annotation_text=f"Avg ${tw_display['exports_usd_bn'].mean():.1f}B",
                                 annotation_font_color="#58a6ff", annotation_position="top left")
                fig_tw.add_hline(y=30, yref="y2", line_dash="dash",
                                 line_color="rgba(63,185,80,0.3)",
                                 annotation_text="30% threshold",
                                 annotation_font_color="rgba(63,185,80,0.6)",
                                 annotation_position="bottom right")
                fig_tw.update_layout(
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#e6edf3", family="monospace"),
                    title="Taiwan → US Exports ($B) + YoY %",
                    xaxis=dict(showgrid=False, tickfont=dict(size=11)),
                    yaxis=dict(showgrid=True, gridcolor="#1c2333", tickprefix="$", ticksuffix="B"),
                    yaxis2=dict(overlaying="y", side="right", showgrid=False, ticksuffix="%",
                                zeroline=True, zerolinecolor="#333"),
                    legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", x=0, y=1.1),
                    height=360, margin=dict(l=10, r=60, t=55, b=10), hovermode="x unified"
                )
                st.plotly_chart(fig_tw, use_container_width=True)
            with tw_c2:
                latest_tw   = tw_display.iloc[-1]
                mom         = round(((tw_display["exports_usd_bn"].iloc[-1] /
                                      tw_display["exports_usd_bn"].iloc[-2]) - 1) * 100, 1)
                yoy_val     = latest_tw["yoy_pct"]
                yoy_color   = "#3fb950" if yoy_val >= 30 else ("#d29922" if yoy_val >= 10 else "#f85149")
                yoy_label   = ("🟢 Strong (>30%)" if yoy_val >= 30 else
                               ("🟡 Moderate" if yoy_val >= 10 else "🔴 Weak (<10%)"))
                trend_r     = tw_display["exports_usd_bn"].iloc[-3:].mean()
                trend_p     = tw_display["exports_usd_bn"].iloc[-6:-3].mean()
                trend_chg   = round(((trend_r/trend_p)-1)*100, 1)
                trend_label = ("🟢 ACCELERATING" if trend_chg > 5 else
                               ("🔴 DECELERATING" if trend_chg < -5 else "🟡 STABLE"))
                st.metric("Latest Month",  f"${latest_tw['exports_usd_bn']:.1f}B",
                          delta=f"{yoy_val:+.1f}% YoY")
                st.metric("MoM Change",    f"{mom:+.1f}%")
                st.metric("14M Peak",      f"${tw_display['exports_usd_bn'].max():.1f}B")
                st.metric("14M Average",   f"${tw_display['exports_usd_bn'].mean():.1f}B")
                st.markdown(f"""<div class="capex-card" style="margin-top:8px">
                    <div class="capex-label">YoY Signal</div>
                    <div style="font-size:14px;font-weight:700;color:{yoy_color}">{yoy_label}</div>
                    <div style="margin-top:8px;padding-top:8px;border-top:1px solid #1c2333">
                    <div class="capex-label">3M Trend</div>
                    <div style="font-size:13px;font-weight:600">{trend_label}</div>
                    <div class="capex-sub">{trend_chg:+.1f}% vs prior 3M</div>
                    </div>
                </div>""", unsafe_allow_html=True)

    with demand_tab2:
        st.markdown("""
**What it is:** Weekly paper submissions to cs.AI + cs.LG + stat.ML on ArXiv — the real-time pulse of global AI research.

**Why it matters:** Research volume is a proxy for researcher headcount funded by capex budgets.
Hiring slows 1–2 quarters before capex cuts appear in earnings. Watch the 4-week moving average —
short-term noise is high. A sustained drop of 10%+ below the 4W MA for 3+ weeks = meaningful signal.
""")
        try:
            arxiv_df = fetch_arxiv_ai_volume(weeks=16)
            if not arxiv_df.empty and arxiv_df["papers"].notna().any():
                arxiv_clean = arxiv_df.dropna(subset=["papers"]).copy()
                arxiv_clean["ma4"] = arxiv_clean["papers"].rolling(4).mean()
                fig_ax = go.Figure()
                fig_ax.add_trace(go.Bar(
                    x=arxiv_clean["week_start"], y=arxiv_clean["papers"],
                    name="Weekly papers", marker_color="#4285f4",
                    hovertemplate="<b>%{x}</b><br>Papers: %{y:,}<extra></extra>"
                ))
                fig_ax.add_trace(go.Scatter(
                    x=arxiv_clean["week_start"], y=arxiv_clean["ma4"],
                    name="4W moving avg", mode="lines",
                    line=dict(color="#ff9900", width=2.5),
                    hovertemplate="4W avg: %{y:,.0f}<extra></extra>"
                ))
                fig_ax.update_layout(
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#e6edf3", family="monospace"),
                    title="ArXiv AI Papers per Week (cs.AI + cs.LG + stat.ML)",
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor="#1c2333"),
                    legend=dict(bgcolor="rgba(0,0,0,0)"),
                    height=320, margin=dict(l=10,r=10,t=44,b=10)
                )
                st.plotly_chart(fig_ax, use_container_width=True)
                ax_k1, ax_k2, ax_k3 = st.columns(3)
                latest_p = int(arxiv_clean["papers"].iloc[-1])
                avg_p    = int(arxiv_clean["papers"].mean())
                peak_p   = int(arxiv_clean["papers"].max())
                ax_k1.metric("Latest week",    f"{latest_p:,}")
                ax_k2.metric("16W average",    f"{avg_p:,}")
                ax_k3.metric("vs 16W peak",    f"{round((latest_p/peak_p-1)*100,1):+.1f}%")
            else:
                st.info("ArXiv data loading — cached for 1hr.")
        except Exception:
            st.info("ArXiv API timeout — will retry on next refresh.")

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # LAYER 2 — BUILD SIGNALS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("## 🏗️ Layer 2 — Build Signals")
    st.caption("Committed physical infrastructure — contracts signed, land acquired, power reserved. Hard to reverse without financial penalty.")

    build_tab1, build_tab2, build_tab3 = st.tabs(["⚡ ERCOT Power Queue", "⚛️ Nuclear PPAs", "🔨 Construction Backlog"])

    with build_tab1:
        st.markdown("""
**What it is:** Companies queuing to connect large new electricity loads to the Texas grid.
Data centers are the #1 driver. Each application is a **2–5 year forward commitment** to build.

**Why it matters:** Power applications must be filed years before a facility opens — this is the
earliest hard evidence of planned data center construction. At 175 GW queued (3× Texas peak demand),
the pipeline is enormous. Watch for **withdrawal rate rising** = companies pulling future plans.
""")
        er1, er2, er3, er4 = st.columns(4)
        er1.metric("Total Queued",     f"{ercot.get('large_load_gw','—')} GW")
        er2.metric("YoY Growth",       f"+{ercot.get('yoy_change_pct','—')}%")
        er3.metric("vs TX Peak Demand","~3×",     help="Texas peak demand ≈55 GW")
        er4.metric("As of",            str(ercot.get("as_of","Q4 2024")))
        ERCOT_HISTORY = [
            ("Q1'22",25),("Q2'22",35),("Q3'22",45),("Q4'22",55),
            ("Q1'23",65),("Q2'23",80),("Q3'23",100),("Q4'23",123),
            ("Q1'24",140),("Q2'24",155),("Q3'24",165),("Q4'24",175),
        ]
        eq_df = pd.DataFrame(ERCOT_HISTORY, columns=["Quarter","GW"])
        fig_er = go.Figure(go.Bar(
            x=eq_df["Quarter"], y=eq_df["GW"],
            marker_color=["#f0883e"]*len(eq_df),
            hovertemplate="<b>%{x}</b><br>%{y} GW queued<extra></extra>"
        ))
        fig_er.add_hline(y=55, line_dash="dash", line_color="#3fb950",
                         annotation_text="TX peak demand ≈55 GW",
                         annotation_font_color="#3fb950")
        fig_er.update_layout(
            title="ERCOT Large Load Interconnection Queue (GW)",
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3", family="monospace"),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#1c2333", ticksuffix=" GW"),
            height=300, margin=dict(l=10,r=10,t=44,b=10)
        )
        st.plotly_chart(fig_er, use_container_width=True)

    with build_tab2:
        st.markdown("""
**Why nuclear PPAs are the ultimate capex signal:** A PPA is a legally binding 20-year contract.
When Microsoft signs a $16B nuclear PPA, they're committing to pay whether they use the power or not.
**This is harder evidence than any earnings guidance.** You cannot PR your way out of a signed PPA.

**What to watch:** New GW announced per quarter. No announcements for 2+ quarters = expansion pausing.
""")
        NUCLEAR_PPAS = [
            ("Microsoft",  "Constellation (Three Mile Island)",  0.835, 20, "Sep 2024", "🟢 Active — 2028",   "$16B · DOE $1B loan secured"),
            ("Meta",       "Constellation (Clinton IL)",         1.1,   20, "Jun 2025", "🟢 Active — 2027",   "1.1 GW · saved from retirement"),
            ("Amazon",     "Talen Energy (Susquehanna PA)",      1.9,   18, "Jun 2025", "🟢 Active — 2026",   "Part of $20B PA investment"),
            ("Meta",       "Vistra (Davis-Besse+Perry+Beaver)",  2.5,   20, "Jan 2026", "🟢 Operating now",   "2.1 GW operating + 0.4 GW uprate"),
            ("Meta",       "TerraPower (Natrium reactors)",      2.6,   25, "Jan 2026", "🟡 Dev — 2032+",     "Up to 8 reactors · NRC review done"),
            ("Meta",       "Oklo Inc (Pike County OH)",          0.4,   20, "Jan 2026", "🟡 Dev — 2030+",     "Broke ground Sep 2025"),
            ("Google",     "Kairos Power (SMR fleet)",           0.5,   15, "Oct 2024", "🟡 Dev — 2030+",     "First GEN IV SMR PPA ever"),
            ("Amazon",     "X-energy (SMR fleet)",               5.0,   20, "Oct 2024", "🟡 Dev — 2034+",     "$500M investment · 5GW by 2039"),
            ("Oracle",     "SMR (3 reactors, undisclosed)",      1.0,   25, "Sep 2024", "🟡 Dev — 2030+",     "Permits secured · location TBD"),
            ("Google",     "NextEra (Iowa nuclear restart)",     0.3,   20, "2025",     "🟡 Feasibility",     "Feasibility study ongoing"),
        ]
        ppa_df = pd.DataFrame(NUCLEAR_PPAS, columns=["Buyer","Seller/Plant","GW","Yrs","Announced","Status","Notes"])
        total_gw  = sum(p[2] for p in NUCLEAR_PPAS)
        active_gw = sum(p[2] for p in NUCLEAR_PPAS if "🟢" in p[5])
        dev_gw    = sum(p[2] for p in NUCLEAR_PPAS if "🟡" in p[5])
        pk1,pk2,pk3,pk4,pk5 = st.columns(5)
        pk1.metric("Total GW Committed",   f"{total_gw:.1f} GW")
        pk2.metric("🟢 Operating / 2028",  f"{active_gw:.1f} GW")
        pk3.metric("🟡 In Development",    f"{dev_gw:.1f} GW")
        pk4.metric("Deals",                f"{len(NUCLEAR_PPAS)}")
        pk5.metric("Buyers",               "5 hyperscalers")
        st.markdown("")
        disp_ppa = ppa_df[["Buyer","Seller/Plant","GW","Yrs","Announced","Status"]].copy()
        disp_ppa["GW"]  = disp_ppa["GW"].apply(lambda x: f"{x:.2f} GW")
        disp_ppa["Yrs"] = disp_ppa["Yrs"].apply(lambda x: f"{x}yr")
        st.dataframe(disp_ppa, use_container_width=True, height=380)
        buyer_gw = {}
        for p in NUCLEAR_PPAS:
            buyer_gw[p[0]] = buyer_gw.get(p[0], 0) + p[2]
        bdf = pd.DataFrame(list(buyer_gw.items()), columns=["Buyer","GW"]).sort_values("GW").set_index("Buyer")
        fig_ppa = go.Figure(go.Bar(
            x=bdf["GW"], y=bdf.index, orientation="h",
            marker_color=["#0668E1","#ffd700","#ff9900","#4285f4","#c0c0c0"],
            hovertemplate="<b>%{y}</b><br>%{x:.1f} GW<extra></extra>"
        ))
        fig_ppa.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3", family="monospace"),
            title="Committed GW by Buyer",
            xaxis=dict(showgrid=True, gridcolor="#1c2333", ticksuffix=" GW"),
            yaxis=dict(showgrid=False),
            height=260, margin=dict(l=10,r=10,t=44,b=10)
        )
        st.plotly_chart(fig_ppa, use_container_width=True)

    with build_tab3:
        st.markdown("""
**What it is:** Signed construction contract backlogs for companies that physically build data centers —
electrical contractors (Quanta, MYR, MasTec), power equipment makers (Powell Industries, Generac),
and power-rack electronics (Vicor). These companies get called when shovels are literally in the ground.

**Supply chain position:**
```
Nuclear PPA signed → ERCOT queue filed → Construction contract awarded (← YOU ARE HERE)
→ Powell switchgear ordered (80–100wk lead) → Vertiv cooling shipped → Vicor modules installed
→ Data center live → Mag7 reports capex
```

**Key signals:**
- **PWR (Quanta) backlog** — largest, most direct: signed contracts for grid + data center electrical work
- **POWL (Powell Industries) lead times** — switchgear ordered 80–100 weeks before data center turns on power
- **VICR (Vicor) revenue** — power modules inside AI server racks; revenue = GPU deployment rate
- **MTZ (MasTec) backlog** — grid + communications infra; less pure-play but tracks utility buildout for DCs
""")
        CONSTRUCTION_BASKET = {
            "PWR":  ("Quanta Services",   "Electrical contractor — grid + data center buildout",  "backlog"),
            "MYRG": ("MYR Group",         "Specialist electrical contractor — data center fit-out","backlog"),
            "MTZ":  ("MasTec",            "Broad infra: power + comms + clean energy",             "backlog"),
            "POWL": ("Powell Industries", "Switchgear & MCC — power room before DC goes live",     "revenue"),
            "GNRC": ("Generac",           "Backup generators — every DC needs N+1 diesel backup",  "revenue"),
            "HON":  ("Honeywell",         "Building automation, HVAC, fire systems for DCs",       "revenue"),
            "VICR": ("Vicor Corp",        "Power modules inside AI server racks — GPU deployment", "revenue"),
        }
        CONSTRUCTION_HARDCODED = {
            # ticker: (latest_q, metric_label, metric_val, qoq_pct, yoy_pct, notes)
            "PWR":  ("Q3'25", "Backlog $B",    39.2,  +9.7,  +34.1, "Record backlog · selected by NiSource for 3GW AI campus"),
            "MYRG": ("Q3'25", "Backlog $B",     3.1,  +4.0,  +18.0, "Data center electrical construction accelerating"),
            "MTZ":  ("Q2'25", "Backlog $B",    16.5, +12.0,  +23.0, "Record backlog · power delivery + comms + clean energy"),
            "POWL": ("FY'25", "Revenue $B",     1.2,  +8.0,  +35.0, "Lead times 80–100 weeks · order backlog at record highs"),
            "GNRC": ("Q3'25", "Revenue $B",     1.07, -2.0,  +6.0,  "Datacenter backup demand rising; residential still dominant"),
            "HON":  ("Q3'25", "Revenue $B",     9.73, +2.0,  +6.0,  "Spinoff of automation business pending — less pure play"),
            "VICR": ("Q3'25", "Revenue $M",    99.5,  +5.0,  +15.0, "Power modules for 48V AI rack architecture — high density"),
        }

        @st.cache_data(ttl=900)
        def fetch_construction_basket(tickers: tuple) -> pd.DataFrame:
            rows = []
            for tk in tickers:
                try:
                    t  = yf.Ticker(tk)
                    qf = t.quarterly_financials
                    if qf is None or qf.empty:
                        raise ValueError("no data")
                    rev_row = None
                    for label in ["Total Revenue", "Revenue"]:
                        if label in qf.index:
                            rev_row = qf.loc[label]
                            break
                    if rev_row is None:
                        raise ValueError("no revenue")
                    rev_row = rev_row.dropna()
                    latest  = float(rev_row.iloc[0])
                    prev_q  = float(rev_row.iloc[1]) if len(rev_row) > 1 else None
                    yoy_q   = float(rev_row.iloc[4]) if len(rev_row) > 4 else None
                    qoq_pct = round(((latest/prev_q)-1)*100,1) if prev_q else None
                    yoy_pct = round(((latest/yoy_q)-1)*100,1)  if yoy_q  else None
                    period  = rev_row.index[0].strftime("%b %Y") if hasattr(rev_row.index[0],'strftime') else str(rev_row.index[0])
                    name, role, _ = CONSTRUCTION_BASKET[tk]
                    rows.append({"Ticker":tk,"Company":name,"Role":role,
                                 "Latest Q":period,"Rev $B":round(latest/1e9,3),
                                 "QoQ %":qoq_pct,"YoY %":yoy_pct})
                except Exception:
                    hc = CONSTRUCTION_HARDCODED.get(tk)
                    if hc:
                        name, role, _ = CONSTRUCTION_BASKET[tk]
                        rows.append({"Ticker":tk,"Company":name,"Role":role,
                                     "Latest Q":hc[0]+" (HC)","Rev $B":hc[2] if hc[2]>10 else hc[2],
                                     "QoQ %":hc[3],"YoY %":hc[4]})
            return pd.DataFrame(rows)

        with st.spinner("Loading construction basket data..."):
            cons_df = fetch_construction_basket(tuple(CONSTRUCTION_BASKET.keys()))

        if not cons_df.empty:
            ck1,ck2,ck3 = st.columns(3)
            valid_yoy = cons_df["YoY %"].dropna()
            ck1.metric("Avg YoY Revenue",  f"{valid_yoy.mean():+.1f}%" if not valid_yoy.empty else "—")
            ck2.metric("YoY Growers",      f"{(valid_yoy>0).sum()}/{len(valid_yoy)}")
            ck3.metric("PWR Backlog",       "$39.2B", delta="+34% YoY")

            cc1, cc2 = st.columns([3,2])
            with cc1:
                chart_c = cons_df[["Company","YoY %"]].dropna().sort_values("YoY %")
                colors_c = ["#3fb950" if v>=20 else ("#d29922" if v>=0 else "#f85149") for v in chart_c["YoY %"]]
                fig_cons = go.Figure(go.Bar(
                    x=chart_c["YoY %"], y=chart_c["Company"], orientation="h",
                    marker_color=colors_c,
                    hovertemplate="<b>%{y}</b><br>YoY: %{x:+.1f}%<extra></extra>"
                ))
                fig_cons.add_vline(x=20, line_dash="dash", line_color="rgba(63,185,80,0.4)",
                                   annotation_text="20% threshold",
                                   annotation_font_color="rgba(63,185,80,0.7)")
                fig_cons.add_vline(x=0, line_color="#555", line_width=1)
                fig_cons.update_layout(
                    title="Construction Basket — YoY Revenue Growth %",
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#e6edf3", family="monospace"),
                    xaxis=dict(showgrid=True, gridcolor="#1c2333", ticksuffix="%"),
                    yaxis=dict(showgrid=False),
                    height=320, margin=dict(l=10,r=20,t=44,b=10)
                )
                st.plotly_chart(fig_cons, use_container_width=True)
            with cc2:
                disp_c = cons_df[["Ticker","Company","Rev $B","QoQ %","YoY %","Latest Q"]].copy()
                disp_c["Rev $B"] = disp_c["Rev $B"].apply(lambda x: f"${x:.2f}B" if pd.notna(x) else "—")
                disp_c["QoQ %"]  = disp_c["QoQ %"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "—")
                disp_c["YoY %"]  = disp_c["YoY %"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "—")
                st.dataframe(disp_c, use_container_width=True, height=320)

            with st.expander("🔍 Company-by-company signal guide", expanded=False):
                for tk, (name, role, mtype) in CONSTRUCTION_BASKET.items():
                    hc = CONSTRUCTION_HARDCODED.get(tk, ("","","","","",""))
                    st.markdown(f"**{tk} — {name}**")
                    st.caption(f"Role: {role} · Latest: {hc[0]} · {hc[1]}: {hc[2]} · QoQ: {hc[3]:+.1f}% · YoY: {hc[4]:+.1f}%")
                    st.caption(f"💡 {hc[5]}")
                    st.markdown("---")

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # LAYER 3 — EQUIPMENT SIGNALS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("## ⚙️ Layer 3 — Equipment Signals")
    st.caption("Equipment ships when data centers are being fitted out. Backlog and book-to-bill ratios are cleaner than stock prices.")

    equip_tab1, equip_tab2 = st.tabs(["📦 Vertiv Order Backlog", "🔬 WFE Basket (Chip Equipment)"])

    with equip_tab1:
        st.markdown("""
**Vertiv** makes power distribution and liquid cooling — every AI data center needs this before it can run GPUs.
**Book-to-bill > 1.0x** means orders arriving faster than shipments = bullish. **< 1.0x** = caution.
Backlog = confirmed purchase orders already on hand — not guidance, not stock price.
""")
        vrt_df = pd.DataFrame(VRT_BACKLOG, columns=["Quarter","Period","Backlog ($B)","Book-to-Bill","Orders YoY %"])
        eq1, eq2 = st.columns([3, 2])
        with eq1:
            fig_vrt = go.Figure()
            fig_vrt.add_trace(go.Bar(
                x=vrt_df["Quarter"], y=vrt_df["Backlog ($B)"],
                name="Backlog $B",
                marker_color=["#3fb950" if b >= 1.0 else "#f85149" for b in vrt_df["Book-to-Bill"]],
                hovertemplate="<b>%{x}</b><br>Backlog: $%{y:.1f}B<extra></extra>"
            ))
            fig_vrt.update_layout(
                title="Vertiv Order Backlog ($B) — green = book-to-bill ≥1.0x",
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#e6edf3", family="monospace"),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor="#1c2333", tickprefix="$", ticksuffix="B"),
                height=300, margin=dict(l=10,r=10,t=44,b=10)
            )
            st.plotly_chart(fig_vrt, use_container_width=True)
        with eq2:
            lv = VRT_BACKLOG[0]; pv = VRT_BACKLOG[1]
            bl_chg    = round(((lv[2]/pv[2])-1)*100,1)
            btb_color = "#3fb950" if lv[3]>=1.2 else ("#d29922" if lv[3]>=1.0 else "#f85149")
            btb_label = "🟢 Expansion" if lv[3]>=1.2 else ("🟡 Neutral" if lv[3]>=1.0 else "🔴 Contraction")
            st.markdown(f"""<div class="capex-card">
                <div class="capex-label"><span class="ticker-badge">VRT</span> Latest: {lv[0]}</div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:8px">
                    <div>
                        <div class="capex-label">Backlog</div>
                        <div style="font-size:22px;font-weight:700;font-family:monospace">${lv[2]:.1f}B</div>
                        <div class="capex-sub"><span style="color:{'#3fb950' if bl_chg>0 else '#f85149'}">
                        {'↑' if bl_chg>0 else '↓'} {abs(bl_chg):.1f}% QoQ</span></div>
                    </div>
                    <div>
                        <div class="capex-label">Book-to-Bill</div>
                        <div style="font-size:22px;font-weight:700;font-family:monospace;color:{btb_color}">{lv[3]:.1f}x</div>
                        <div class="capex-sub">{btb_label}</div>
                    </div>
                </div>
                <div style="margin-top:10px;padding-top:10px;border-top:1px solid #1c2333">
                    <div class="capex-label">Organic Orders YoY</div>
                    <div style="font-size:18px;font-weight:700;font-family:monospace;color:#3fb950">+{lv[4]}%</div>
                </div>
                <div class="capex-sub" style="margin-top:8px">Next earnings: ~Apr 2026 · Updated quarterly</div>
            </div>""", unsafe_allow_html=True)

    with equip_tab2:
        st.markdown("""
**The 7 companies that make the machines that make AI chips.** Revenue = fabs actually buying equipment to ramp capacity.
ASML (EUV monopoly) + TEL + AMAT + Lam = ~75% of global WFE market.

⚠️ **China distortion:** ~35–50% of Japanese equipment exports went to China stockpiling in 2024.
TEL/Screen high but ASML flat = China-driven, not AI. All 7 high together = genuine AI fab cycle.
Advantest accelerating = GPU shipment rate accelerating (testing is last step before shipping).
""")
        WFE_BASKET = {
            "8035.T": ("Tokyo Electron",   "🇯🇵", "Etch+coater/developer · advanced logic & HBM"),
            "6857.T": ("Advantest",         "🇯🇵", "Chip testing · every GPU tested here (58% share)"),
            "6146.T": ("Disco",             "🇯🇵", "Dicing/grinding · CoWoS & HBM packaging"),
            "7735.T": ("Screen Holdings",   "🇯🇵", "Wafer cleaning · fab utilization proxy"),
            "ASML":   ("ASML",              "🇳🇱", "EUV lithography · monopoly · purest AI signal"),
            "AMAT":   ("Applied Materials", "🇺🇸", "Deposition & etch · largest WFE globally"),
            "LRCX":   ("Lam Research",      "🇺🇸", "Etch & deposition · HBM & NAND stacking"),
        }

        @st.cache_data(ttl=3600)
        def fetch_wfe_basket(tickers: tuple) -> pd.DataFrame:
            rows = []
            for tk in tickers:
                try:
                    t = yf.Ticker(tk)
                    qf = t.quarterly_financials
                    if qf is None or qf.empty: continue
                    rev_row = None
                    for label in ["Total Revenue","Revenue"]:
                        if label in qf.index:
                            rev_row = qf.loc[label]; break
                    if rev_row is None: continue
                    rev_row = rev_row.dropna()
                    latest  = float(rev_row.iloc[0])
                    prev_q  = float(rev_row.iloc[1]) if len(rev_row)>1 else None
                    yoy_q   = float(rev_row.iloc[4]) if len(rev_row)>4 else None
                    qoq_pct = round(((latest/prev_q)-1)*100,1) if prev_q else None
                    yoy_pct = round(((latest/yoy_q)-1)*100,1)  if yoy_q  else None
                    period  = rev_row.index[0].strftime("%b %Y") if hasattr(rev_row.index[0],'strftime') else str(rev_row.index[0])
                    name, flag, role = WFE_BASKET[tk]
                    rows.append({"Ticker":tk,"Company":f"{flag} {name}","Role":role,
                                 "Period":period,"Rev $B":round(latest/1e9,3),
                                 "QoQ %":qoq_pct,"YoY %":yoy_pct})
                except Exception:
                    continue
            return pd.DataFrame(rows)

        with st.spinner("Loading WFE basket..."):
            wfe_df = fetch_wfe_basket(tuple(WFE_BASKET.keys()))

        if wfe_df is not None and not wfe_df.empty:
            valid_yoy_w = wfe_df["YoY %"].dropna()
            wk1,wk2,wk3,wk4 = st.columns(4)
            wk1.metric("Companies loaded",  f"{len(wfe_df)}/{len(WFE_BASKET)}")
            wk2.metric("Avg YoY",           f"{valid_yoy_w.mean():+.1f}%" if not valid_yoy_w.empty else "—",
                       help=">20% = strong AI equipment cycle")
            wk3.metric("Avg QoQ",           f"{wfe_df['QoQ %'].dropna().mean():+.1f}%")
            wk4.metric("YoY Growers",       f"{(valid_yoy_w>0).sum()}/{len(valid_yoy_w)}")

            wc1, wc2 = st.columns([3,2])
            with wc1:
                chart_w = wfe_df[["Company","YoY %"]].dropna().sort_values("YoY %")
                colors_w = ["#3fb950" if v>=20 else ("#d29922" if v>=0 else "#f85149") for v in chart_w["YoY %"]]
                fig_wfe = go.Figure(go.Bar(
                    x=chart_w["YoY %"], y=chart_w["Company"], orientation="h",
                    marker_color=colors_w,
                    hovertemplate="<b>%{y}</b><br>YoY: %{x:+.1f}%<extra></extra>"
                ))
                fig_wfe.add_vline(x=20, line_dash="dash", line_color="rgba(63,185,80,0.4)",
                                  annotation_text="20%", annotation_font_color="rgba(63,185,80,0.7)")
                fig_wfe.add_vline(x=0, line_color="#555", line_width=1)
                fig_wfe.update_layout(
                    title="WFE Basket — Latest Quarter YoY Revenue %",
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#e6edf3", family="monospace"),
                    xaxis=dict(showgrid=True, gridcolor="#1c2333", ticksuffix="%"),
                    yaxis=dict(showgrid=False),
                    height=320, margin=dict(l=10,r=20,t=44,b=10)
                )
                st.plotly_chart(fig_wfe, use_container_width=True)
            with wc2:
                disp_w = wfe_df[["Ticker","Company","Rev $B","QoQ %","YoY %","Period"]].copy()
                disp_w["Rev $B"] = disp_w["Rev $B"].apply(lambda x: f"${x:.2f}B" if pd.notna(x) else "—")
                disp_w["QoQ %"]  = disp_w["QoQ %"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "—")
                disp_w["YoY %"]  = disp_w["YoY %"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "—")
                st.dataframe(disp_w, use_container_width=True, height=320)
        else:
            st.warning("WFE data unavailable — JP tickers may be rate-limited. Retry in a moment.")

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # LAYER 4 — CONFIRMATION SIGNALS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("## 📊 Layer 4 — Confirmation Signals")
    st.caption("Backward-looking but authoritative. By the time these move, the market already knows. Use them to SIZE the cycle, not TIME it.")

    confirm_tab1, confirm_tab2 = st.tabs(["💰 Mag7 Capital Expenditure", "🌐 Arista Revenue (Network Switches)"])

    with confirm_tab1:
        st.markdown("""
**The most direct measure of AI spending — but always backward-looking.**
Two consecutive QoQ declines = serious signal. Use this to confirm what Layer 1–3 already told you.
""")
        refresh_cols = st.columns([3, 1])
        with refresh_cols[1]:
            refresh_live = st.button("🔄 Refresh Live Data", key="mag7_refresh")

        if refresh_live:
            with st.spinner("Fetching live capex data from yfinance..."):
                live_capex = fetch_mag7_live_capex(tuple(MAG7_CAPEX_HISTORY.keys()))
        else:
            live_capex = {}

        company_sel = st.multiselect(
            "Select companies",
            list(MAG7_CAPEX_HISTORY.keys()),
            default=list(MAG7_CAPEX_HISTORY.keys()),
            format_func=lambda x: f"{x} — {MAG7_CAPEX_HISTORY[x]['name']}"
        )
        chart_type = st.radio("Chart type", ["Stacked Area","Grouped Bar","Line"], horizontal=True, key="mag7_chart")

        sel_data = {k: MAG7_CAPEX_HISTORY[k] for k in company_sel if k in MAG7_CAPEX_HISTORY}
        if sel_data:
            all_quarters = []
            for v in sel_data.values():
                for q in v["quarters"]:
                    if q[0] not in all_quarters:
                        all_quarters.append(q[0])
            all_quarters = sorted(set(all_quarters), key=lambda x: (x[-2:], x[:2]))

            fig_m7 = go.Figure()
            for ticker, info in sel_data.items():
                q_map  = {q[0]: q[2] for q in info["quarters"]}
                vals   = [q_map.get(q, None) for q in all_quarters]
                if chart_type == "Grouped Bar":
                    fig_m7.add_trace(go.Bar(name=f"{ticker}",
                        x=all_quarters, y=vals,
                        marker_color=info["color"],
                        hovertemplate=f"<b>{info['name']}</b><br>%{{x}}: $%{{y:.1f}}B<extra></extra>"))
                elif chart_type == "Stacked Area":
                    fig_m7.add_trace(go.Scatter(name=f"{ticker}",
                        x=all_quarters, y=vals, stackgroup="one",
                        line=dict(color=info["color"], width=1.5),
                        hovertemplate=f"<b>{info['name']}</b><br>%{{x}}: $%{{y:.1f}}B<extra></extra>"))
                else:
                    fig_m7.add_trace(go.Scatter(name=f"{ticker}",
                        x=all_quarters, y=vals, mode="lines+markers",
                        line=dict(color=info["color"], width=2),
                        marker=dict(size=6),
                        hovertemplate=f"<b>{info['name']}</b><br>%{{x}}: $%{{y:.1f}}B<extra></extra>"))

            if chart_type == "Grouped Bar":
                fig_m7.update_layout(barmode="group")
            fig_m7.update_layout(
                title=f"Mag7 Quarterly Capex ($B) — {chart_type}",
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font=dict(color="#e6edf3", family="monospace"),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor="#1c2333", tickprefix="$", ticksuffix="B"),
                legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h"),
                height=380, margin=dict(l=10,r=10,t=44,b=10), hovermode="x unified"
            )
            st.plotly_chart(fig_m7, use_container_width=True)

        m7_cols = st.columns(min(len(sel_data), 4))
        for i, (ticker, info) in enumerate(sel_data.items()):
            if info["quarters"]:
                lq = info["quarters"][0]
                pq = info["quarters"][1] if len(info["quarters"]) > 1 else None
                qoq = round(((lq[2]/pq[2])-1)*100,1) if pq else 0
                c_color = "#3fb950" if qoq > 0 else "#f85149"
                with m7_cols[i % 4]:
                    st.markdown(f"""<div class="capex-card">
                        <div class="capex-label"><span class="ticker-badge">{ticker}</span> {info['name']}</div>
                        <div style="font-size:20px;font-weight:700;font-family:monospace">${lq[2]:.1f}B</div>
                        <div class="capex-sub">{lq[0]} · <span style="color:{c_color}">{qoq:+.1f}% QoQ</span></div>
                        <div class="capex-sub" style="margin-top:4px;font-size:10px">{info.get('guidance','')}</div>
                    </div>""", unsafe_allow_html=True)

        m7t = st.columns([1,1])
        with m7t[0]:
            st.metric("Mag7 Combined Latest Q",  f"${mag7_latest_total:.0f}B",  delta=f"{mag7_qoq:+.1f}% QoQ")
        with m7t[1]:
            st.caption("Hardcoded from 10-Q/10-K filings. Refresh quarterly after earnings season (Jan/Apr/Jul/Oct).")

    with confirm_tab2:
        st.markdown("""
**Arista makes the 400G/800G Ethernet switches inside hyperscaler AI clusters.**
Revenue = switches shipping = racks going live. No switch orders = no new AI racks.
Revenue growth deceleration below 20% YoY = fewer new GPU clusters being commissioned.
""")
        @st.cache_data(ttl=3600)
        def fetch_anet_revenue():
            try:
                t   = yf.Ticker("ANET")
                qf  = t.quarterly_financials
                if qf is None or qf.empty: return None
                rev_row = qf.loc["Total Revenue"] if "Total Revenue" in qf.index else None
                if rev_row is None: return None
                quarters = []
                for i in range(min(8, len(rev_row))):
                    rev  = float(rev_row.iloc[i]) / 1e9
                    prev = float(rev_row.iloc[i+1]) / 1e9 if i+1 < len(rev_row) else None
                    yoy  = float(rev_row.iloc[i+4]) / 1e9 if i+4 < len(rev_row) else None
                    quarters.append({
                        "Period": rev_row.index[i].strftime("%b %Y") if hasattr(rev_row.index[i],'strftime') else str(rev_row.index[i]),
                        "Rev $B": round(rev, 3),
                        "QoQ %":  round(((rev/prev)-1)*100,1) if prev else None,
                        "YoY %":  round(((rev/yoy)-1)*100,1)  if yoy  else None,
                    })
                return pd.DataFrame(quarters)
            except Exception:
                return None

        with st.spinner("Loading Arista revenue..."):
            anet_df = fetch_anet_revenue()

        if anet_df is not None and not anet_df.empty:
            ac1, ac2 = st.columns([3,2])
            with ac1:
                fig_anet = go.Figure()
                fig_anet.add_trace(go.Bar(
                    x=anet_df["Period"], y=anet_df["Rev $B"],
                    name="Revenue $B", marker_color="#4285f4",
                    hovertemplate="<b>%{x}</b><br>$%{y:.3f}B<extra></extra>"
                ))
                if anet_df["YoY %"].notna().any():
                    yoy_colors_a = ["#3fb950" if (v or 0)>=20 else ("#d29922" if (v or 0)>=0 else "#f85149")
                                    for v in anet_df["YoY %"]]
                    fig_anet.add_trace(go.Scatter(
                        x=anet_df["Period"], y=anet_df["YoY %"],
                        name="YoY %", yaxis="y2", mode="lines+markers",
                        line=dict(color="#3fb950", width=2),
                        marker=dict(size=6, color=yoy_colors_a),
                        hovertemplate="YoY: %{y:+.1f}%<extra></extra>"
                    ))
                fig_anet.add_hline(y=20, yref="y2", line_dash="dash",
                                   line_color="rgba(63,185,80,0.3)",
                                   annotation_text="20% threshold",
                                   annotation_font_color="rgba(63,185,80,0.6)")
                fig_anet.update_layout(
                    title="Arista Quarterly Revenue ($B) + YoY %",
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font=dict(color="#e6edf3", family="monospace"),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor="#1c2333", tickprefix="$", ticksuffix="B"),
                    yaxis2=dict(overlaying="y", side="right", showgrid=False, ticksuffix="%"),
                    legend=dict(bgcolor="rgba(0,0,0,0)"),
                    height=320, margin=dict(l=10,r=60,t=44,b=10)
                )
                st.plotly_chart(fig_anet, use_container_width=True)
            with ac2:
                latest_a = anet_df.iloc[0]
                yoy_a    = latest_a["YoY %"] or 0
                ac_color = "#3fb950" if yoy_a>=25 else ("#d29922" if yoy_a>=10 else "#f85149")
                st.metric("Latest Revenue",  f"${latest_a['Rev $B']:.2f}B",
                          delta=f"{yoy_a:+.1f}% YoY")
                st.metric("QoQ Change",      f"{latest_a['QoQ %']:+.1f}%" if pd.notna(latest_a['QoQ %']) else "—")
                st.markdown(f"""<div class="capex-card" style="margin-top:8px">
                    <div class="capex-label">AI Rack Signal</div>
                    <div style="font-size:14px;font-weight:700;color:{ac_color}">
                    {'🟢 Strong (>25% YoY)' if yoy_a>=25 else '🟡 Moderate (10–25%)' if yoy_a>=10 else '🔴 Slowing (<10%)'}
                    </div>
                    <div class="capex-sub" style="margin-top:6px">
                    Deceleration below 20% YoY = fewer new AI racks deploying<br>
                    Q1'26 guidance: ~$2.6B (+28% YoY)
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Arista revenue data loading — retry in a moment.")


st.divider()
st.caption("⚠️ Data from Yahoo Finance, ERCOT, ArXiv, Taiwan MoF. Mag7 capex hardcoded from earnings filings — refresh quarterly. Not financial advice.")
