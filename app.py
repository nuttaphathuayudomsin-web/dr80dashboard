import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import json
import re
import requests
import base64
import psycopg2
from psycopg2.extras import RealDictCursor

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
</style>
""", unsafe_allow_html=True)

# ── INDICES  (primary ticker + fallbacks tried in order) ─────────────────────
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

# ── SECTOR ETFs ───────────────────────────────────────────────────────────────
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

# ── STOCK UNIVERSE — expanded pools per market ────────────────────────────────
# Each entry: ticker → (name, sector, sub-sector)
US_STOCKS = {
    # Technology
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
    # Consumer
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
    "MO":   ("Altria Group",          "Consumer Defensive","Tobacco"),
    "KHC":  ("Kraft Heinz",           "Consumer Defensive","Food"),
    "GIS":  ("General Mills",         "Consumer Defensive","Food"),
    "CL":   ("Colgate-Palmolive",     "Consumer Defensive","Household Products"),
    # Financials
    "JPM":  ("JPMorgan Chase",        "Financials",   "Banks"),
    "BAC":  ("Bank of America",       "Financials",   "Banks"),
    "WFC":  ("Wells Fargo",           "Financials",   "Banks"),
    "GS":   ("Goldman Sachs",         "Financials",   "Investment Banking"),
    "MS":   ("Morgan Stanley",        "Financials",   "Investment Banking"),
    "BLK":  ("BlackRock Inc",         "Financials",   "Asset Management"),
    "V":    ("Visa Inc",              "Financials",   "Payment Networks"),
    "MA":   ("Mastercard",            "Financials",   "Payment Networks"),
    "AXP":  ("American Express",      "Financials",   "Credit Cards"),
    "C":    ("Citigroup",             "Financials",   "Banks"),
    "SCHW": ("Charles Schwab",        "Financials",   "Brokerage"),
    "CB":   ("Chubb Ltd",             "Financials",   "Insurance"),
    "PGR":  ("Progressive Corp",      "Financials",   "Insurance"),
    "MET":  ("MetLife Inc",           "Financials",   "Life Insurance"),
    "AIG":  ("American Intl Group",   "Financials",   "Insurance"),
    # Healthcare
    "JNJ":  ("Johnson & Johnson",     "Healthcare",   "Pharma / Medical"),
    "UNH":  ("UnitedHealth Group",    "Healthcare",   "Health Insurance"),
    "LLY":  ("Eli Lilly",             "Healthcare",   "Pharma / GLP-1"),
    "PFE":  ("Pfizer Inc",            "Healthcare",   "Pharma"),
    "ABBV": ("AbbVie Inc",            "Healthcare",   "Biopharma"),
    "MRK":  ("Merck & Co",            "Healthcare",   "Pharma"),
    "BMY":  ("Bristol-Myers Squibb",  "Healthcare",   "Biopharma"),
    "AMGN": ("Amgen Inc",             "Healthcare",   "Biotech"),
    "GILD": ("Gilead Sciences",       "Healthcare",   "Antiviral / Biotech"),
    "ISRG": ("Intuitive Surgical",    "Healthcare",   "Robotic Surgery"),
    "CVS":  ("CVS Health",            "Healthcare",   "Pharmacy / Insurance"),
    "HUM":  ("Humana Inc",            "Healthcare",   "Health Insurance"),
    "TMO":  ("Thermo Fisher",         "Healthcare",   "Lab Equipment"),
    "DHR":  ("Danaher Corp",          "Healthcare",   "Life Sciences Tools"),
    "MRNA": ("Moderna Inc",           "Healthcare",   "mRNA Biotech"),
    # Energy
    "XOM":  ("ExxonMobil",            "Energy",       "Oil & Gas Integrated"),
    "CVX":  ("Chevron Corp",          "Energy",       "Oil & Gas Integrated"),
    "COP":  ("ConocoPhillips",        "Energy",       "Oil & Gas E&P"),
    "SLB":  ("SLB (Schlumberger)",    "Energy",       "Oilfield Services"),
    "EOG":  ("EOG Resources",         "Energy",       "Oil & Gas E&P"),
    "PSX":  ("Phillips 66",           "Energy",       "Refining"),
    "MPC":  ("Marathon Petroleum",    "Energy",       "Refining"),
    "OXY":  ("Occidental Petroleum",  "Energy",       "Oil & Gas E&P"),
    "HAL":  ("Halliburton",           "Energy",       "Oilfield Services"),
    "BKR":  ("Baker Hughes",          "Energy",       "Oilfield Services"),
    # Communication
    "NFLX": ("Netflix Inc",           "Communication","Streaming"),
    "DIS":  ("Walt Disney Co",        "Communication","Media / Entertainment"),
    "T":    ("AT&T Inc",              "Communication","Telecom"),
    "VZ":   ("Verizon Communications","Communication","Telecom"),
    "CMCSA":("Comcast Corp",          "Communication","Cable / Media"),
    "WBD":  ("Warner Bros Discovery", "Communication","Media / Streaming"),
    "PARA": ("Paramount Global",      "Communication","Media"),
    "FOX":  ("Fox Corp",              "Communication","Media"),
    # Industrials
    "GE":   ("GE Aerospace",          "Industrials",  "Aerospace / Defense"),
    "BA":   ("Boeing Co",             "Industrials",  "Aerospace"),
    "RTX":  ("RTX Corp",              "Industrials",  "Defense / Aerospace"),
    "LMT":  ("Lockheed Martin",       "Industrials",  "Defense"),
    "NOC":  ("Northrop Grumman",      "Industrials",  "Defense"),
    "HON":  ("Honeywell Intl",        "Industrials",  "Conglomerate / Automation"),
    "CAT":  ("Caterpillar Inc",       "Industrials",  "Heavy Machinery"),
    "DE":   ("Deere & Company",       "Industrials",  "Agricultural Machinery"),
    "UPS":  ("UPS",                   "Industrials",  "Logistics"),
    "FDX":  ("FedEx Corp",            "Industrials",  "Logistics"),
    # Materials / Real Estate / Utilities
    "LIN":  ("Linde PLC",             "Basic Materials","Industrial Gases"),
    "APD":  ("Air Products",          "Basic Materials","Industrial Gases"),
    "NEM":  ("Newmont Corp",          "Basic Materials","Gold Mining"),
    "FCX":  ("Freeport-McMoRan",      "Basic Materials","Copper Mining"),
    "AMT":  ("American Tower",        "Real Estate",  "Cell Tower REIT"),
    "PLD":  ("Prologis",              "Real Estate",  "Industrial REIT"),
    "SPG":  ("Simon Property Group",  "Real Estate",  "Mall REIT"),
    "NEE":  ("NextEra Energy",        "Utilities",    "Renewable Energy"),
    "DUK":  ("Duke Energy",           "Utilities",    "Electric Utility"),
    "SO":   ("Southern Company",      "Utilities",    "Electric Utility"),
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
    "003550.KS":("LG Corp",            "Industrials",  "Holding Company"),
    "028260.KS":("Samsung C&T",        "Industrials",  "Trading / Construction"),
    "009150.KS":("Samsung Electro-Mech","Technology",  "Electronic Components"),
    "000830.KS":("Samsung Fire & Marine","Financials", "Insurance"),
    "012330.KS":("Hyundai Mobis",      "Consumer Cyclical","Auto Parts"),
    "066570.KS":("LG Electronics",     "Consumer Cyclical","Home Appliances / Electronics"),
    "105560.KS":("KB Financial Group", "Financials",   "Banks"),
    "055550.KS":("Shinhan Financial",  "Financials",   "Banks"),
    "086790.KS":("Hana Financial",     "Financials",   "Banks"),
    "000100.KS":("Yuhan Corp",         "Healthcare",   "Pharma"),
    "011170.KS":("Lotte Chemical",     "Basic Materials","Petrochemicals"),
    "010950.KS":("S-Oil Corp",         "Energy",       "Oil Refining"),
    "032830.KS":("Samsung Life",       "Financials",   "Life Insurance"),
    "096770.KS":("SK Innovation",      "Energy",       "Oil / EV Battery"),
    "017670.KS":("SK Telecom",         "Communication","Telecom"),
    "030200.KS":("KT Corp",            "Communication","Telecom"),
    "034730.KS":("SK Inc",             "Industrials",  "Holding Company"),
    "000810.KS":("Samsung Fire",       "Financials",   "Insurance"),
    "271560.KS":("Orion Corp",         "Consumer Defensive","Snacks / Food"),
    "139480.KS":("Imarketkorea",       "Industrials",  "IT Distribution"),
}

JP_STOCKS = {
    "7203.T": ("Toyota Motor",         "Consumer Cyclical","Automobiles"),
    "9984.T": ("SoftBank Group",       "Technology",   "Venture / Telecom"),
    "6861.T": ("Keyence Corp",         "Technology",   "Industrial Automation"),
    "8306.T": ("Mitsubishi UFJ",       "Financials",   "Banks"),
    "6758.T": ("Sony Group",           "Consumer Cyclical","Electronics / Gaming"),
    "9432.T": ("NTT Corp",             "Communication","Telecom"),
    "7974.T": ("Nintendo",             "Consumer Cyclical","Gaming"),
    "6902.T": ("Denso Corp",           "Consumer Cyclical","Auto Parts"),
    "8035.T": ("Tokyo Electron",       "Technology",   "Semiconductor Equipment"),
    "9433.T": ("KDDI Corp",            "Communication","Telecom"),
    "4502.T": ("Takeda Pharmaceutical","Healthcare",   "Pharma"),
    "6501.T": ("Hitachi Ltd",          "Industrials",  "Conglomerate / IT"),
    "7267.T": ("Honda Motor",          "Consumer Cyclical","Automobiles"),
    "8411.T": ("Mizuho Financial",     "Financials",   "Banks"),
    "9022.T": ("Central Japan Railway","Industrials",  "Transportation"),
    "6367.T": ("Daikin Industries",    "Industrials",  "HVAC / Air Conditioning"),
    "4063.T": ("Shin-Etsu Chemical",   "Basic Materials","Specialty Chemicals"),
    "8316.T": ("Sumitomo Mitsui",      "Financials",   "Banks"),
    "6981.T": ("Murata Manufacturing", "Technology",   "Electronic Components"),
    "9983.T": ("Fast Retailing",       "Consumer Cyclical","Apparel Retail"),
    "4519.T": ("Chugai Pharmaceutical","Healthcare",   "Biotech / Pharma"),
    "7751.T": ("Canon Inc",            "Technology",   "Imaging / Office Equipment"),
    "6954.T": ("Fanuc Corp",           "Industrials",  "Industrial Robots"),
    "5108.T": ("Bridgestone Corp",     "Consumer Cyclical","Tires"),
    "8001.T": ("Itochu Corp",          "Industrials",  "Trading Conglomerate"),
}

HK_STOCKS = {
    "0700.HK":("Tencent Holdings",    "Technology",   "Gaming / Social"),
    "9988.HK":("Alibaba Group",        "Technology",   "E-Commerce / Cloud"),
    "1299.HK":("AIA Group",            "Financials",   "Life Insurance"),
    "0005.HK":("HSBC Holdings",        "Financials",   "Banks"),
    "2318.HK":("Ping An Insurance",    "Financials",   "Insurance"),
    "3690.HK":("Meituan",              "Consumer Cyclical","Food Delivery"),
    "0941.HK":("China Mobile",         "Communication","Telecom"),
    "1398.HK":("ICBC",                 "Financials",   "Banks"),
    "2628.HK":("China Life Insurance", "Financials",   "Life Insurance"),
    "0388.HK":("HKEX",                 "Financials",   "Exchange / Markets"),
    "1810.HK":("Xiaomi Corp",          "Technology",   "Consumer Electronics"),
    "2269.HK":("WuXi Biologics",       "Healthcare",   "Biotech / CRO"),
    "0883.HK":("CNOOC Ltd",            "Energy",       "Oil & Gas"),
    "0016.HK":("Sun Hung Kai Prop",    "Real Estate",  "Property Developer"),
    "1109.HK":("China Resources Land", "Real Estate",  "Property Developer"),
    "2020.HK":("ANTA Sports",          "Consumer Cyclical","Sportswear"),
    "9618.HK":("JD.com",               "Consumer Cyclical","E-Commerce"),
    "3988.HK":("Bank of China",        "Financials",   "Banks"),
    "0011.HK":("Hang Seng Bank",       "Financials",   "Banks"),
    "0175.HK":("Geely Automobile",     "Consumer Cyclical","Automobiles / EV"),
}

SH_STOCKS = {
    "600519.SS":("Kweichow Moutai",    "Consumer Defensive","Premium Spirits"),
    "601318.SS":("Ping An Insurance",  "Financials",   "Insurance"),
    "600036.SS":("China Merchants Bank","Financials",  "Banks"),
    "601166.SS":("Industrial Bank",    "Financials",   "Banks"),
    "600900.SS":("Yangtze Power",      "Utilities",    "Hydropower"),
    "601628.SS":("China Life Insurance","Financials",  "Life Insurance"),
    "600030.SS":("CITIC Securities",   "Financials",   "Brokerage"),
    "601398.SS":("ICBC",               "Financials",   "Banks"),
    "600276.SS":("Hengrui Medicine",   "Healthcare",   "Pharma / Oncology"),
    "601288.SS":("Agricultural Bank",  "Financials",   "Banks"),
    "601888.SS":("China Tourism Group","Consumer Cyclical","Duty-Free Retail"),
    "600585.SS":("Anhui Conch Cement", "Basic Materials","Cement"),
    "601012.SS":("Longi Green Energy", "Utilities",    "Solar Energy"),
    "600104.SS":("SAIC Motor",         "Consumer Cyclical","Automobiles"),
    "601601.SS":("China Pacific Ins",  "Financials",   "Insurance"),
}

SZ_STOCKS = {
    "000858.SZ":("Wuliangye Yibin",   "Consumer Defensive","Premium Spirits"),
    "000333.SZ":("Midea Group",        "Consumer Cyclical","Home Appliances"),
    "002594.SZ":("BYD Co",             "Consumer Cyclical","EV / Batteries"),
    "000001.SZ":("Ping An Bank",       "Financials",   "Banks"),
    "300750.SZ":("CATL",               "Basic Materials","EV Batteries"),
    "001979.SZ":("China Merchants Shekou","Real Estate","Property Developer"),
    "000651.SZ":("Gree Electric",      "Consumer Cyclical","Home Appliances"),
    "002415.SZ":("Hikvision",          "Technology",   "Video Surveillance / AI"),
    "000725.SZ":("BOE Technology",     "Technology",   "Display Panels"),
    "300059.SZ":("East Money Info",    "Financials",   "Fintech / Brokerage"),
    "300274.SZ":("Sungrow Power",      "Utilities",    "Solar Inverters"),
    "002475.SZ":("Luxshare Precision", "Technology",   "Electronics Manufacturing"),
    "000568.SZ":("Luzhou Laojiao",     "Consumer Defensive","Premium Spirits"),
    "002714.SZ":("Muyuan Foods",       "Consumer Defensive","Pork / Agriculture"),
    "300760.SZ":("Mindray Medical",    "Healthcare",   "Medical Devices"),
}

TH_STOCKS = {
    "PTT.BK":    ("PTT PCL",           "Energy",       "Oil & Gas (State)"),
    "ADVANC.BK": ("Advanced Info Svc", "Communication","Telecom"),
    "CPALL.BK":  ("CP All PCL",        "Consumer Defensive","Convenience Retail"),
    "AOT.BK":    ("Airports of Thailand","Industrials","Airport Operator"),
    "SCC.BK":    ("SCG (Siam Cement)", "Basic Materials","Cement / Conglomerate"),
    "KBANK.BK":  ("Kasikornbank",      "Financials",   "Banks"),
    "SCB.BK":    ("SCB Group",         "Financials",   "Banks"),
    "BBL.BK":    ("Bangkok Bank",      "Financials",   "Banks"),
    "TRUE.BK":   ("True Corp",         "Communication","Telecom / Digital"),
    "GULF.BK":   ("Gulf Energy Dev",   "Utilities",    "Power Generation"),
    "PTTEP.BK":  ("PTT E&P",           "Energy",       "Oil & Gas E&P"),
    "BDMS.BK":   ("Bangkok Dusit Med", "Healthcare",   "Private Hospitals"),
    "BH.BK":     ("Bumrungrad Hosp",   "Healthcare",   "Private Hospitals"),
    "CPF.BK":    ("Charoen Pokphand Foods","Consumer Defensive","Food / Agriculture"),
    "DTAC.BK":   ("Total Access Comm", "Communication","Telecom"),
}

VN_STOCKS = {
    "VCB.VN":    ("Vietcombank",       "Financials",   "Banks"),
    "BID.VN":    ("BIDV",              "Financials",   "Banks"),
    "VHM.VN":    ("Vinhomes",          "Real Estate",  "Property Developer"),
    "HPG.VN":    ("Hoa Phat Group",    "Basic Materials","Steel / Industrial"),
    "VNM.VN":    ("Vinamilk",          "Consumer Defensive","Dairy / Food"),
    "TCB.VN":    ("Techcombank",       "Financials",   "Banks"),
    "FPT.VN":    ("FPT Corp",          "Technology",   "IT Services / Telecom"),
    "MWG.VN":    ("Mobile World",      "Consumer Cyclical","Electronics Retail"),
    "MSN.VN":    ("Masan Group",       "Consumer Defensive","FMCG / Mining"),
    "GAS.VN":    ("PetroVietnam Gas",  "Energy",       "Gas Distribution"),
    "CTG.VN":    ("VietinBank",        "Financials",   "Banks"),
    "MBB.VN":    ("MB Bank",           "Financials",   "Banks"),
    "VIC.VN":    ("Vingroup",          "Real Estate",  "Conglomerate / Property"),
    "SSI.VN":    ("SSI Securities",    "Financials",   "Brokerage"),
    "VPB.VN":    ("VPBank",            "Financials",   "Banks"),
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

# ── Helpers ───────────────────────────────────────────────────────────────────
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

# ── Data functions ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=900)
def get_index_performance(tickers: list):
    """Try each ticker in order, return first successful result with ticker used."""
    today = datetime.today()
    for ticker in tickers:
        try:
            t  = yf.Ticker(ticker)
            h2 = t.history(period="2d")
            if len(h2) < 2:
                continue
            def pct(days):
                h = t.history(start=(today - timedelta(days=days)).strftime("%Y-%m-%d"))
                if len(h) < 2: return None
                return round(((h["Close"].iloc[-1] / h["Close"].iloc[0]) - 1) * 100, 2)
            price = h2["Close"].iloc[-1]
            d1    = round(((h2["Close"].iloc[-1] / h2["Close"].iloc[-2]) - 1) * 100, 2)
            ytd_h = t.history(start=datetime(today.year, 1, 1).strftime("%Y-%m-%d"))
            ytd   = round(((ytd_h["Close"].iloc[-1] / ytd_h["Close"].iloc[0]) - 1) * 100, 2) if len(ytd_h) > 1 else None
            return {"price": price, "1D": d1, "30D": pct(30), "YTD": ytd,
                    "3Y": pct(365*3), "5Y": pct(365*5), "source_ticker": ticker}
        except:
            continue
    return None  # all tickers failed

@st.cache_data(ttl=900)
def get_sector_etf_perf(etf_map: tuple):
    """Fetch performance for sector ETFs. etf_map is tuple of (sector, ticker) pairs."""
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
        except:
            continue
    return pd.DataFrame(rows).set_index("Sector") if rows else pd.DataFrame()

@st.cache_data(ttl=900)
def get_stocks_data(stock_dict_items: tuple, history_arg: str, period_label: str):
    """
    stock_dict_items: tuple of (ticker, (name, sector, sub)) pairs
    Fetches all tickers and returns full DataFrame — filtering happens AFTER in UI.
    """
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
        except:
            continue
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
                parts, src_t = title_t.rsplit(" - ",1), title_t.rsplit(" - ",1)[-1].strip()
                title_t = title_t.rsplit(" - ",1)[0].strip()
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
    except:
        return []

# ── App ────────────────────────────────────────────────────────────────────────
st.title("📈 Global Market Monitor")
st.caption(f"Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ·  Yahoo Finance + Google News  ·  Cache 15 min")

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=15 * 60 * 1000, key="autorefresh")
except ImportError:
    pass

tab1, tab2, tab3, tab4, tab5 = st.tabs(["🌐 Indices", "🏭 US Sectors", "🔍 Stock Screener", "📰 News", "🇹🇭 DR Tracker"])

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
                                 "source_ticker": d.get("source_ticker", "")})
        prog.empty()
    failed = [n for n in INDICES if n not in [r["Index"] for r in idx_rows]]
    if failed:
        st.warning(f"⚠️ Could not load data for: {', '.join(failed)} — Yahoo Finance may be temporarily unavailable for these. Will retry on next refresh.")

    if idx_rows:
        df_idx = pd.DataFrame(idx_rows).set_index("Index")
        cols = st.columns(5)
        for i, (name, row) in enumerate(df_idx.iterrows()):
            with cols[i % 5]:
                src = f" ({row.get('source_ticker','')})" if row.get("source_ticker","").startswith("EW") else ""
                st.metric(name + src, f"{row['Price']:,.2f}",
                          delta=f"{row['1D']:+.2f}%" if row["1D"] is not None else "—")
        st.write("")
        styled_idx = (df_idx[["1D","30D","YTD","3Y","5Y"]].style
                      .applymap(color_pct)
                      .format(lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"))
        st.dataframe(styled_idx, use_container_width=True, height=390)
    else:
        st.error("Could not load any index data. Check internet connection.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — US SECTOR ETFs  (Morningstar-style)
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("US Sector Returns  ·  SPDR Sector ETFs")
    st.caption("Tracks the same 11 GICS sectors as Morningstar US Sector Returns table, using SPDR ETF prices via Yahoo Finance.")

    etf_items = tuple(SECTOR_ETFS.items())
    with st.spinner("Loading sector ETFs..."):
        df_sec = get_sector_etf_perf(etf_items)

    if not df_sec.empty:
        # Bar chart — YTD sorted
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

        # Best / worst callout
        ytd_valid = df_sec["YTD"].dropna()
        if not ytd_valid.empty:
            best_s  = ytd_valid.idxmax()
            worst_s = ytd_valid.idxmin()
            c1,c2,c3 = st.columns(3)
            c1.metric("🏆 Best YTD",  best_s,  f"{ytd_valid[best_s]:+.2f}%")
            c2.metric("📉 Worst YTD", worst_s, f"{ytd_valid[worst_s]:+.2f}%")
            c3.metric("📊 Avg YTD",   "All sectors", f"{ytd_valid.mean():+.2f}%")
    else:
        st.error("Could not load sector ETF data.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — STOCK SCREENER  (fixed: full pool fetched first, then filter)
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Stock Screener")

    c1,c2,c3,c4,c5 = st.columns([2,2,2,2,1])
    with c1: market_choice = st.selectbox("Market",    list(MARKET_STOCK_MAP.keys()))
    with c2: timeframe     = st.selectbox("Timeframe", list(PERIOD_MAP.keys()))
    with c3: screen_mode   = st.selectbox("Screen by", [
                "🔥 Top Active (Value)","🚀 Top Gainers","📉 Top Losers","📊 All (by Change)"])
    with c4:
        stock_dict   = MARKET_STOCK_MAP[market_choice]
        avail_sectors = ["All Sectors"] + sorted(set(v[1] for v in stock_dict.values()))
        sector_filter = st.selectbox("Sector", avail_sectors)
    with c5: top_n = st.selectbox("Show", [10, 20, 30])

    history_arg, period_label = PERIOD_MAP[timeframe]
    change_col  = f"Change ({period_label})"
    value_col   = "Value Traded"

    # Pass full stock dict as tuple of items for caching
    stock_items = tuple(stock_dict.items())

    with st.spinner(f"Loading {market_choice} — full universe ({len(stock_items)} stocks)..."):
        df_all = get_stocks_data(stock_items, history_arg, period_label)

    if df_all.empty:
        st.warning("No data returned. This market may have limited Yahoo Finance coverage.")
    else:
        # 1. Filter by sector FIRST (on full pool)
        if sector_filter != "All Sectors":
            df_filtered = df_all[df_all["Sector"] == sector_filter].copy()
        else:
            df_filtered = df_all.copy()

        available = len(df_filtered)

        # 2. Sort / rank
        if screen_mode == "🔥 Top Active (Value)":
            df_out = df_filtered.nlargest(top_n, value_col)
        elif screen_mode == "🚀 Top Gainers":
            df_out = df_filtered.nlargest(top_n, change_col)
        elif screen_mode == "📉 Top Losers":
            df_out = df_filtered.nsmallest(top_n, change_col)
        else:
            df_out = df_filtered.sort_values(change_col, ascending=False).head(top_n)

        df_out = df_out.reset_index(drop=True)
        df_out.index += 1
        df_out.index.name = "Rank"

        # Summary
        if not df_out.empty:
            gainers = (df_out[change_col] > 0).sum()
            losers  = (df_out[change_col] < 0).sum()
            avg_chg = df_out[change_col].mean()
            best    = df_out.loc[df_out[change_col].idxmax(), "Company"]
            m1,m2,m3,m4,m5 = st.columns(5)
            m1.metric("Pool size",        f"{available} stocks")
            m2.metric("Showing",          len(df_out))
            m3.metric("🟢 Gainers",       gainers)
            m4.metric("🔴 Losers",        losers)
            m5.metric(f"Avg {period_label}", f"{avg_chg:+.2f}%")
            st.write("")

        display = df_out.copy()
        display[value_col] = display[value_col].apply(fmt_value)
        display["Volume"]  = display["Volume"].apply(fmt_value)
        display["Price"]   = display["Price"].apply(lambda x: f"{x:,.2f}")

        styled = (display.style
                  .applymap(color_pct, subset=[change_col])
                  .format({change_col: lambda x: f"{x:+.2f}%" if isinstance(x,(int,float)) else x}))
        st.dataframe(styled, use_container_width=True, height=min(120+len(df_out)*36, 650))

        csv   = df_out.to_csv()
        fname = f"{market_choice.split()[1]}_{screen_mode.split()[1]}_{timeframe}_{datetime.now().strftime('%Y%m%d')}.csv"
        st.download_button("⬇️ Download CSV", data=csv, file_name=fname, mime="text/csv")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — NEWS
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Market News Feed")
    nc1,nc2 = st.columns([3,1])
    with nc1: news_cat   = st.selectbox("Category", list(NEWS_CATEGORIES.keys()))
    with nc2: news_count = st.selectbox("Articles", [10,15,20])
    custom_q = st.text_input("Search specific topic / stock", placeholder="e.g. NVIDIA earnings, Fed rate, Thailand baht...")
    query    = custom_q.strip() if custom_q.strip() else NEWS_CATEGORIES[news_cat]

    with st.spinner("Fetching news..."):
        articles = fetch_news(query, max_items=news_count)

    if not articles:
        st.warning("Could not fetch news. Check internet or try a different search.")
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

st.divider()
st.caption("⚠️ Data from Yahoo Finance. Vietnam & Thailand may have limited coverage. Not financial advice.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — KTB DR MARKET SHARE TRACKER
# Persistent storage: Google Sheets (survives Streamlit Cloud restarts/sleeps)
# Data source: SET Marketplace API (delay feed — free)
# Fallback: manual entry when API is unavailable
# ══════════════════════════════════════════════════════════════════════════════

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
KTB_PREFIX   = "80"

# ── Supabase / Postgres connection ────────────────────────────────────────────
@st.cache_resource
def _get_conn():
    """Open one persistent DB connection for the session. Returns (conn, err)."""
    try:
        conn = psycopg2.connect(SUPABASE_URL, connect_timeout=10)
        conn.autocommit = True
        return conn, None
    except Exception as e:
        return None, str(e)

def _cursor():
    """Return a fresh cursor, reconnecting if the connection dropped."""
    conn, err = _get_conn()
    if err:
        return None, err
    try:
        # Ping — reconnect if stale
        conn.cursor().execute("SELECT 1")
    except Exception:
        _get_conn.clear()
        conn, err = _get_conn()
        if err:
            return None, err
    return conn.cursor(cursor_factory=RealDictCursor), None

def db_ensure_table():
    """Create the dr_daily table if it doesn't exist yet."""
    cur, err = _cursor()
    if err:
        return
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dr_daily (
            date         TEXT PRIMARY KEY,
            week_label   TEXT,
            total_dr     INTEGER DEFAULT 0,
            ktb_dr       INTEGER DEFAULT 0,
            set_vol      DOUBLE PRECISION DEFAULT 0,
            set_val      DOUBLE PRECISION DEFAULT 0,
            dr_vol       DOUBLE PRECISION DEFAULT 0,
            dr_val       DOUBLE PRECISION DEFAULT 0,
            ktb_vol      DOUBLE PRECISION DEFAULT 0,
            ktb_val      DOUBLE PRECISION DEFAULT 0,
            source       TEXT DEFAULT 'manual',
            captured_at  TEXT
        )
    """)

# ── Data helpers ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def db_load() -> pd.DataFrame:
    cur, err = _cursor()
    if err:
        return pd.DataFrame()
    try:
        cur.execute("SELECT * FROM dr_daily ORDER BY date DESC")
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        for col in ["total_dr", "ktb_dr"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in ["set_vol","set_val","dr_vol","dr_val","ktb_vol","ktb_val"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df.reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

def db_upsert(row: dict) -> bool:
    cur, err = _cursor()
    if err:
        st.error(f"Database connection failed: {err}")
        return False
    try:
        cur.execute("""
            INSERT INTO dr_daily
                (date,week_label,total_dr,ktb_dr,set_vol,set_val,
                 dr_vol,dr_val,ktb_vol,ktb_val,source,captured_at)
            VALUES
                (%(date)s,%(week_label)s,%(total_dr)s,%(ktb_dr)s,%(set_vol)s,%(set_val)s,
                 %(dr_vol)s,%(dr_val)s,%(ktb_vol)s,%(ktb_val)s,%(source)s,%(captured_at)s)
            ON CONFLICT (date) DO UPDATE SET
                week_label  = EXCLUDED.week_label,
                total_dr    = EXCLUDED.total_dr,
                ktb_dr      = EXCLUDED.ktb_dr,
                set_vol     = EXCLUDED.set_vol,
                set_val     = EXCLUDED.set_val,
                dr_vol      = EXCLUDED.dr_vol,
                dr_val      = EXCLUDED.dr_val,
                ktb_vol     = EXCLUDED.ktb_vol,
                ktb_val     = EXCLUDED.ktb_val,
                source      = EXCLUDED.source,
                captured_at = EXCLUDED.captured_at
        """, row)
        db_load.clear()
        return True
    except Exception as e:
        st.error(f"Failed to save: {e}")
        return False

def db_delete(date_str: str):
    cur, err = _cursor()
    if err:
        return
    try:
        cur.execute("DELETE FROM dr_daily WHERE date = %s", (date_str,))
        db_load.clear()
    except Exception as e:
        st.error(f"Delete failed: {e}")

def week_label(d: datetime) -> str:
    jan4  = datetime(d.year, 1, 4)
    delta = (d - jan4).days + 4
    wn    = max(1, (delta) // 7 + 1)
    return f"{d.year}-W{wn:02d}"

# ── SET API fetch ─────────────────────────────────────────────────────────────
# ── Weekly aggregation ────────────────────────────────────────────────────────
def make_weekly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["week_label"] = df["week_label"].fillna(df["date"].apply(lambda d: week_label(datetime.strptime(d, "%Y-%m-%d"))))
    g = df.groupby("week_label")
    wk = pd.DataFrame({
        "Week":               g["week_label"].first(),
        "Days":               g["date"].count(),
        "From":               g["date"].min(),
        "To":                 g["date"].max(),
        "Avg DR Listings":    g["total_dr"].mean().round(1),
        "Avg KTB DR":         g["ktb_dr"].mean().round(1),
        "KTB Vol (shs)":      g["ktb_vol"].sum(),
        "KTB Val ('000 THB)": g["ktb_val"].sum(),
        "DR Vol (shs)":       g["dr_vol"].sum(),
        "SET Vol (shs)":      g["set_vol"].sum(),
    }).reset_index(drop=True)
    wk["KTB % of DR Vol"]  = (wk["KTB Vol (shs)"]  / wk["DR Vol (shs)"].replace(0, float("nan")) * 100).round(2)
    wk["KTB % of SET Vol"] = (wk["KTB Vol (shs)"]  / wk["SET Vol (shs)"].replace(0, float("nan")) * 100).round(2)
    wk["KTB Listing %"]    = (wk["Avg KTB DR"]      / wk["Avg DR Listings"].replace(0, float("nan")) * 100).round(2)
    return wk.sort_values("Week", ascending=False).reset_index(drop=True)

# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("🇹🇭 KTB DR (Code 80) — SET Market Share Tracker")
    st.caption("Enter data daily from set.or.th after market close (17:30 BKK). Stored permanently in Supabase.")

    # ── DB connection check ───────────────────────────────────────────────────
    if not SUPABASE_URL:
        st.error("⚠️ **Database not connected.** Add `SUPABASE_URL` to your Streamlit secrets.")
        with st.expander("📋 Setup Guide"):
            st.markdown("""
**1.** Go to [supabase.com](https://supabase.com) → create free account → new project (Singapore region)

**2.** Click **Connect** at top of project → copy the **URI** connection string

**3.** In Streamlit Cloud → App Settings → Secrets, paste:
```toml
SUPABASE_URL = "postgresql://postgres:YOUR_PASSWORD@db.XXXX.supabase.co:5432/postgres"
```
Table is created automatically on first load.
            """)
        st.stop()

    # ── Ensure table exists ───────────────────────────────────────────────────
    db_ensure_table()

    # ── Status bar ───────────────────────────────────────────────────────────
    df_hist = db_load()
    s1, s2, s3 = st.columns(3)
    s1.metric("Days captured", len(df_hist))
    if not df_hist.empty:
        s2.metric("Latest entry", df_hist["date"].iloc[0])
        latest_ktb_pct = (df_hist["ktb_vol"].iloc[0] / df_hist["dr_vol"].iloc[0] * 100
                          if df_hist["dr_vol"].iloc[0] > 0 else 0)
        s3.metric("KTB % of DR Vol (latest)", f"{latest_ktb_pct:.2f}%")

    st.divider()

    # ── Live KTB DR Price Table ───────────────────────────────────────────────
    st.markdown("#### 📡 KTB DR Live Prices (Yahoo Finance)")
    st.caption("Symbols follow the pattern `{UNDERLYING}80.BK` on SET. Prices delayed ~15 min.")

    # Known KTB DR underlyings — update this list as new ones are listed
    KTB_UNDERLYINGS = [
        "AAPL","AMZN","NVDA","TSLA","META","MSFT","GOOG","GOOGL","NFLX",
        "AMD","AVGO","QCOM","INTC","ORCL","CRM","NOW","ADBE","SNOW","PLTR",
        "SHOP","UBER","LYFT","HOOD","COIN","DDOG","CRWD","PANW","CSCO",
        "IBM","DELL","HPQ","MU","MRVL","AMAT","LRCX","KLAC","ASML",
        "MA","V","PYPL","BKNG","ABNB","EXPE","TRVUS",
        "JNJ","LLY","AMGN","ABBV","PFE","UNH","ISRG","BDX",
        "TSMC","TSM","SONY","NINTENDO","TOYOTA","HONDA","SOFTBANK",
        "BABA","JD","BIDU","TENCENT","MEITUAN","XIAOMI","SMIC",
        "WMT","COSTCO","NKE","SBUX","MCD","KO","PEP",
        "GS","MS","JPM","BAC","BLK",
        "GOLD","GLD","NEM","ZIJIN",
        "DBS","UOB","GRAB","SEA","SINGTEL",
    ]

    @st.cache_data(ttl=300)  # refresh every 5 min
    def fetch_ktb_dr_prices(underlyings: tuple) -> pd.DataFrame:
        """Fetch all KTB DR prices from Yahoo Finance using batch download."""
        tickers = [f"{u}80.BK" for u in underlyings]
        try:
            raw = yf.download(
                tickers, period="2d", interval="1d",
                auto_adjust=True, progress=False, threads=True
            )
            if raw.empty:
                return pd.DataFrame()

            rows = []
            close = raw["Close"] if "Close" in raw else raw.get("close", pd.DataFrame())
            vol   = raw["Volume"] if "Volume" in raw else raw.get("volume", pd.DataFrame())

            for sym in tickers:
                try:
                    last_close = close[sym].dropna().iloc[-1] if sym in close.columns else None
                    prev_close = close[sym].dropna().iloc[-2] if sym in close.columns and len(close[sym].dropna()) > 1 else None
                    volume     = vol[sym].dropna().iloc[-1]   if sym in vol.columns   else 0
                    if last_close is None:
                        continue
                    chg    = last_close - prev_close if prev_close else None
                    chg_pct= (chg / prev_close * 100) if prev_close else None
                    underlying = sym.replace("80.BK", "")
                    rows.append({
                        "Symbol":      sym,
                        "Underlying":  underlying,
                        "Price (THB)": round(float(last_close), 4),
                        "Chg":         round(float(chg), 4)    if chg    is not None else None,
                        "Chg %":       round(float(chg_pct), 2) if chg_pct is not None else None,
                        "Volume":      int(volume) if volume else 0,
                    })
                except Exception:
                    continue
            return pd.DataFrame(rows)
        except Exception as e:
            st.warning(f"Yahoo Finance fetch failed: {e}")
            return pd.DataFrame()

    if st.button("🔄 Refresh Prices", key="refresh_prices"):
        fetch_ktb_dr_prices.clear()

    with st.spinner("Fetching KTB DR prices from Yahoo Finance…"):
        price_df = fetch_ktb_dr_prices(tuple(KTB_UNDERLYINGS))

    if price_df.empty:
        st.info("No price data returned — market may be closed or symbols not yet listed.")
    else:
        # Sort by volume descending so most-traded appear first
        price_df = price_df.sort_values("Volume", ascending=False).reset_index(drop=True)

        def color_chg(val):
            if isinstance(val, float) and pd.notna(val):
                return "color: #3fb950" if val > 0 else ("color: #f85149" if val < 0 else "")
            return ""

        styled_prices = (
            price_df.style
            .applymap(color_chg, subset=["Chg", "Chg %"])
            .format({
                "Price (THB)": "{:.4f}",
                "Chg":         lambda x: f"{x:+.4f}" if pd.notna(x) else "—",
                "Chg %":       lambda x: f"{x:+.2f}%" if pd.notna(x) else "—",
                "Volume":      "{:,.0f}",
            })
        )
        st.dataframe(styled_prices, use_container_width=True,
                     height=min(80 + len(price_df) * 35, 600))
        st.caption(f"Showing {len(price_df)} KTB DR symbols with price data. Cached 5 min — click Refresh to update.")


    st.markdown("#### ✏️ Enter Today's Data")
    st.caption("Upload a screenshot of the SET DR page — AI will read the numbers for you, or fill in manually.")

    # ── AI screenshot extraction ──────────────────────────────────────────────
    ANTHROPIC_API_KEY = st.secrets.get("ANTHROPIC_API_KEY", "")

    def extract_from_screenshot(img_bytes: bytes) -> dict | None:
        """Send screenshot to Claude vision API, return extracted numbers as dict."""
        if not ANTHROPIC_API_KEY:
            return None
        b64 = base64.standard_b64encode(img_bytes).decode("utf-8")
        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 512,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {"type": "base64", "media_type": "image/png", "data": b64}
                    },
                    {
                        "type": "text",
                        "text": (
                            "This is a screenshot of the SET Thailand DR market data page. "
                            "Extract the following numbers and return ONLY a JSON object with these exact keys:\n"
                            "- total_dr: total number of DR securities listed (integer)\n"
                            "- ktb_dr: count of DR symbols starting with '80' (KTB DRs) (integer)\n"
                            "- dr_vol: total DR trading volume in shares (float)\n"
                            "- dr_val: total DR trading value in thousands THB (float)\n"
                            "- ktb_vol: KTB DR (80) trading volume in shares (float)\n"
                            "- ktb_val: KTB DR (80) trading value in thousands THB (float)\n"
                            "- set_vol: total SET market volume in shares if visible (float, 0 if not shown)\n"
                            "- set_val: total SET market value in thousands THB if visible (float, 0 if not shown)\n"
                            "Return ONLY the JSON, no explanation. Example: {\"total_dr\":150,\"ktb_dr\":8,...}"
                        )
                    }
                ]
            }]
        }
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json=payload, timeout=30
            )
            text = r.json()["content"][0]["text"].strip()
            # Strip markdown code fences if present
            text = re.sub(r"```json|```", "", text).strip()
            return json.loads(text)
        except Exception as e:
            st.warning(f"AI extraction failed: {e} — please fill in manually.")
            return None

    # ── Upload widget ─────────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        "📸 Upload SET DR page screenshot (PNG/JPG)",
        type=["png", "jpg", "jpeg"],
        help="Take a screenshot of set.or.th/th/market/product/dr/marketdata and upload here"
    )

    # Pre-fill values from AI extraction or defaults
    prefill = {}
    if uploaded is not None:
        if not ANTHROPIC_API_KEY:
            st.warning("⚠️ Add `ANTHROPIC_API_KEY` to Streamlit secrets to enable AI extraction. Fill in manually for now.")
        else:
            with st.spinner("🤖 Reading numbers from screenshot…"):
                prefill = extract_from_screenshot(uploaded.read()) or {}
            if prefill:
                st.success("✅ AI extracted the numbers — review below and click Save.")

    # ── Entry form (pre-filled if AI extracted) ───────────────────────────────
    with st.form("entry_form", clear_on_submit=True):
        m_date     = st.date_input("📅 Date", value=datetime.today())
        st.markdown("**DR Listings**")
        lc1, lc2   = st.columns(2)
        m_total_dr = lc1.number_input("Total DR listings",        min_value=0,   value=int(prefill.get("total_dr", 150)), step=1)
        m_ktb_dr   = lc2.number_input("KTB DR (code 80) listings",min_value=0,   value=int(prefill.get("ktb_dr", 8)),   step=1)
        st.markdown("**Volume & Value**")
        vc1, vc2, vc3 = st.columns(3)
        m_dr_vol   = vc1.number_input("Total DR Vol (shares)",    min_value=0.0, value=float(prefill.get("dr_vol", 0.0)),  step=1e6, format="%.0f")
        m_ktb_vol  = vc2.number_input("KTB DR Vol (shares)",      min_value=0.0, value=float(prefill.get("ktb_vol", 0.0)), step=1e4, format="%.0f")
        m_dr_val   = vc3.number_input("Total DR Val ('000 THB)",  min_value=0.0, value=float(prefill.get("dr_val", 0.0)),  step=1e3, format="%.0f")
        vc4, vc5, vc6 = st.columns(3)
        m_ktb_val  = vc4.number_input("KTB DR Val ('000 THB)",    min_value=0.0, value=float(prefill.get("ktb_val", 0.0)), step=1e3, format="%.0f")
        m_set_vol  = vc5.number_input("Total SET Vol (shares)",   min_value=0.0, value=float(prefill.get("set_vol", 0.0)), step=1e8, format="%.0f")
        m_set_val  = vc6.number_input("Total SET Val ('000 THB)", min_value=0.0, value=float(prefill.get("set_val", 0.0)), step=1e6, format="%.0f")

        source = "screenshot" if prefill else "manual"
        submitted = st.form_submit_button("💾 Save Entry", use_container_width=True, type="primary")
        if submitted:
            d_str = m_date.strftime("%Y-%m-%d")
            ok = db_upsert({
                "date":        d_str,
                "week_label":  week_label(datetime(m_date.year, m_date.month, m_date.day)),
                "total_dr":    int(m_total_dr),
                "ktb_dr":      int(m_ktb_dr),
                "set_vol":     float(m_set_vol),
                "set_val":     float(m_set_val),
                "dr_vol":      float(m_dr_vol),
                "dr_val":      float(m_dr_val),
                "ktb_vol":     float(m_ktb_vol),
                "ktb_val":     float(m_ktb_val),
                "source":      source,
                "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            if ok:
                st.success(f"✅ Saved {d_str}  |  KTB DR: {m_ktb_dr} listings, {m_ktb_vol:,.0f} shares")
                st.rerun()

    # ── Data display ─────────────────────────────────────────────────────────
    df_hist = db_load()

    if df_hist.empty:
        st.info("No data yet — fill in the form above to start tracking.")
    else:
        df = df_hist.copy()
        df["KTB Listing %"]    = (df["ktb_dr"]  / df["total_dr"].replace(0, float("nan")) * 100).round(2)
        df["KTB % of DR Vol"]  = (df["ktb_vol"] / df["dr_vol"].replace(0, float("nan"))   * 100).round(2)
        df["KTB % of SET Vol"] = (df["ktb_vol"] / df["set_vol"].replace(0, float("nan"))  * 100).round(2)
        df["KTB % of DR Val"]  = (df["ktb_val"] / df["dr_val"].replace(0, float("nan"))   * 100).round(2)

        # ── KPI strip ─────────────────────────────────────────────────────────
        st.markdown("#### 📊 Latest Day")
        latest = df.iloc[0]
        k1,k2,k3,k4,k5,k6 = st.columns(6)
        k1.metric("Total DR",         f"{int(latest['total_dr']):,}")
        k2.metric("KTB DR (80)",       f"{int(latest['ktb_dr']):,}")
        k3.metric("KTB Listing %",     f"{latest['KTB Listing %']:.2f}%"    if pd.notna(latest['KTB Listing %'])    else "—")
        k4.metric("KTB % of DR Vol",   f"{latest['KTB % of DR Vol']:.2f}%"  if pd.notna(latest['KTB % of DR Vol'])  else "—")
        k5.metric("KTB % of SET Vol",  f"{latest['KTB % of SET Vol']:.2f}%" if pd.notna(latest['KTB % of SET Vol']) else "—")
        k6.metric("KTB % of DR Val",   f"{latest['KTB % of DR Val']:.2f}%"  if pd.notna(latest['KTB % of DR Val'])  else "—")

        st.divider()

        # ── Charts ────────────────────────────────────────────────────────────
        if len(df) >= 2:
            st.markdown("#### 📈 Trends")
            tc1, tc2 = st.columns(2)
            with tc1:
                st.markdown("**KTB DR Market Share % (Volume)**")
                chart_df = df[["date","KTB % of DR Vol","KTB % of SET Vol"]].dropna().set_index("date").sort_index()
                st.line_chart(chart_df, height=220)
            with tc2:
                st.markdown("**DR Listings: Total vs KTB**")
                listing_df = df[["date","total_dr","ktb_dr"]].set_index("date").sort_index()
                st.line_chart(listing_df, height=220)

        st.divider()

        # ── Weekly summary ────────────────────────────────────────────────────
        st.markdown("#### 📅 Weekly Summary")
        wk_df = make_weekly(df)
        if not wk_df.empty:
            st.dataframe(
                wk_df.style.format({
                    "Avg DR Listings":    "{:.1f}",
                    "Avg KTB DR":         "{:.1f}",
                    "KTB Vol (shs)":      "{:,.0f}",
                    "KTB Val ('000 THB)": "{:,.0f}",
                    "DR Vol (shs)":       "{:,.0f}",
                    "SET Vol (shs)":      "{:,.0f}",
                    "KTB % of DR Vol":    lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                    "KTB % of SET Vol":   lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                    "KTB Listing %":      lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                }),
                use_container_width=True,
                height=min(60 + len(wk_df)*36, 500)
            )

        st.divider()

        # ── Daily history ─────────────────────────────────────────────────────
        st.markdown("#### 🗓️ Daily History")
        disp = df[["date","total_dr","ktb_dr","KTB Listing %",
                   "ktb_vol","dr_vol","set_vol",
                   "KTB % of DR Vol","KTB % of SET Vol","source"]].rename(columns={
            "date":"Date","total_dr":"Total DR","ktb_dr":"KTB DR",
            "KTB Listing %":"KTB Listing %","ktb_vol":"KTB Vol",
            "dr_vol":"DR Vol","set_vol":"SET Vol",
            "KTB % of DR Vol":"KTB % DR Vol",
            "KTB % of SET Vol":"KTB % SET Vol","source":"Source"
        })
        st.dataframe(
            disp.style.format({
                "KTB Vol":      "{:,.0f}",
                "DR Vol":       "{:,.0f}",
                "SET Vol":      "{:,.0f}",
                "KTB Listing %": lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                "KTB % DR Vol":  lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
                "KTB % SET Vol": lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
            }),
            use_container_width=True,
            height=min(120 + len(df)*36, 500)
        )

        # ── Delete & download ─────────────────────────────────────────────────
        dl1, dl2 = st.columns([3,1])
        with dl1:
            csv_out = disp.to_csv(index=False)
            st.download_button("⬇️ Download CSV", data=csv_out,
                               file_name=f"ktb_dr_{datetime.now().strftime('%Y%m%d')}.csv",
                               mime="text/csv")
        with dl2:
            with st.expander("🗑️ Delete row"):
                del_date = st.selectbox("Date", df["date"].tolist(), label_visibility="collapsed")
                if st.button("Delete", type="primary"):
                    db_delete(del_date)
                    st.rerun()

    st.divider()
    st.caption("📌 Data source: set.or.th/th/market/product/dr/marketdata — enter after 17:30 BKK each trading day")

