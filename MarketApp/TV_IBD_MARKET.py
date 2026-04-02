import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import io
from datetime import datetime
import yfinance as yf
from streamlit_lightweight_charts import renderLightweightCharts
from tradingview_screener import Query, Column

# ==========================================
# 📁 הגדרות נתיבים ופונקציות עזר
# ==========================================
DATA_DIR = "data"

def find_file_robust(directory, filename_target):
    if not os.path.exists(directory):
        return None
    try:
        files = os.listdir(directory)
        target = filename_target.lower().replace(" ", "").strip()
        for f in files:
            clean_f = f.lower().replace(" ", "").strip()
            if clean_f == target:
                return os.path.join(directory, f)
    except:
        pass
    return None

# ==========================================
# ⚙️ הגדרות עמוד ותצורה
# ==========================================
st.set_page_config(
    page_title="Terminal :: Hybrid Market",
    page_icon="📟",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
    .stApp { background-color: #0E1117; }
    .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; direction: rtl; max-width: 98%; }
    h1, h2, h3 { color: #E6EDF3; font-family: 'Consolas', 'Courier New', monospace; text-transform: uppercase; letter-spacing: 1px; }
    h1 { border-bottom: 2px solid #238636; padding-bottom: 10px; }
    .stDataFrame { direction: ltr; }
    div[data-baseweb="input"] { background-color: #161B22; border: 1px solid #30363D; border-radius: 4px; }
    div[data-baseweb="select"] > div { background-color: #161B22; border: 1px solid #30363D; }
    .stDownloadButton > button { background-color: #238636; color: white; border: none; width: 100%; }
    .stDownloadButton > button:hover { background-color: #2EA043; border: none; }
    .diagnostic-box { background-color: #161B22; border: 1px solid #30363D; padding: 15px; border-radius: 5px; margin-bottom: 20px;}
    .disclaimer { font-size: 0.8em; color: #8B949E; text-align: center; margin-top: 50px; padding-top: 20px; border-top: 1px solid #30363D;}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 📡 טעינת נתונים משולבת (TV + IBD + Group Ranking)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def load_hybrid_data():
    debug_log = []
    try:
        # 1. TradingView Live Data
        query = (Query()
                 .set_markets('america')
                 .select('name', 'close', 'open', 'high', 'low', 'volume', 'average_volume_10d_calc', 
                         'market_cap_basic', 'sector', 'industry', 
                         'SMA10', 'SMA20', 'SMA50', 'SMA200', 'price_52_week_high', 'price_52_week_low',
                         'Perf.W', 'Perf.1M', 'Perf.3M', 'Perf.Y', 'ATR')
                 .where(Column('close') > 1, Column('average_volume_10d_calc') > 100000)
                 .limit(4500)) 
        
        count, df_tv = query.get_scanner_data()
        if df_tv.empty: 
            return pd.DataFrame(), pd.DataFrame(), ["❌ לא התקבלו נתונים מ-TV"]

        rename_map = {'ticker': 'Symbol', 'name': 'Company_Name', 'close': 'Price', 'volume': 'TV_Volume', 
                      'average_volume_10d_calc': 'TV_AvgVol10', 'market_cap_basic': 'Market Cap', 
                      'industry': 'Industry Group Name'}
        df_raw = df_tv.rename(columns=rename_map).copy()
        df_raw['Symbol'] = df_raw['Symbol'].apply(lambda x: x.split(':')[-1] if isinstance(x, str) and ':' in x else x)
        df_raw['TV_Link'] = "https://www.tradingview.com/chart/?symbol=" + df_raw['Symbol']
        df_raw['Market_Cap_B'] = pd.to_numeric(df_raw['Market Cap'], errors='coerce') / 1_000_000_000.0
        df_raw['Dollar_Volume_M'] = (df_raw['Price'] * df_raw['TV_AvgVol10']) / 1_000_000.0

        # 2. Live Pattern Engine
        df_raw['Rel_Volume'] = df_raw['TV_Volume'] / df_raw['TV_AvgVol10']
        df_raw['Spread'] = df_raw['high'] - df_raw['low']
        df_raw['Close_Pos'] = np.where(df_raw['Spread'] > 0, (df_raw['Price'] - df_raw['low']) / df_raw['Spread'], 0.5)
        df_raw['ADR_Pct'] = np.where(df_raw['low'] > 0, (df_raw['ATR'] / df_raw['low']) * 100, 0)

        def get_patterns(row):
            b = []
            p, op, hi, lo = row.get('Price', 0), row.get('open', 0), row.get('high', 0), row.get('low', 0)
            rvol, atr, adr = row.get('Rel_Volume', 1), row.get('ATR', 0), row.get('ADR_Pct', 0)
            sma10, sma20, sma50, sma200 = row.get('SMA10', 0), row.get('SMA20', 0), row.get('SMA50', 0), row.get('SMA200', 0)
            spread, cp = row.get('Spread', 0), row.get('Close_Pos', 0.5)
            
            perf3 = row.get('Perf.3M', 0)
            perf3 = (perf3 / 100.0) if not pd.isna(perf3) else 0
            
            h52 = row.get('price_52_week_high', 0)
            h52p = (p - h52) / h52 if h52 and h52 > 0 else -99

            if p <= 0: return ""
            if sma50 > 0 and lo < sma50 < p: b.append("U&R(50) 🛡️")
            if sma20 > 0 and lo < sma20 < p: b.append("U&R(21) 🛡️")
            if sma50 > 0 and (0.0 <= (p - sma50) / sma50 <= 0.03) and p > sma200: b.append("Bounce50 🏀")
            if sma20 > 0 and sma50 > 0 and (0.0 <= (p - sma20) / sma20 <= 0.035) and sma20 > sma50: b.append("Ride20 🏄")
            if rvol > 1.5 and p > op and cp > 0.7: b.append("HVC 🚀")
            if rvol > 1.2 and adr > 0 and (spread / lo * 100 if lo > 0 else 0) > adr and cp < 0.4: b.append("SQUAT 🏋️")
            if atr > 0 and spread < (atr * 0.7) and cp > 0.5: b.append("ID 🕯️")
            if perf3 > 0.90 and h52p >= -0.20: b.append("HTF 🚩")
            if adr > 0 and (spread / lo * 100 if lo > 0 else adr) < (adr * 0.6) and rvol < 1.0: b.append("Tight/VCP 🤏")
            if h52p >= -0.02: b.append("52W High 👑")
            if sma10 > 0 and (p / sma10 - 1) > 0.15: b.append("EXT ⚠️")
            
            move_pct = spread / lo * 100 if lo > 0 else 0
            adr_ratio = move_pct / adr if adr > 0 else 0
            if 0.8 <= adr_ratio <= 1.2: b.append("1 ADR 📏")
            elif 1.5 <= adr_ratio <= 2.5: b.append("2 ADR 🔥")

            return "  ".join(b)

        df_raw['Pattern_Badges'] = df_raw.apply(get_patterns, axis=1)

        # 3. Weinstein Stages
        p, ma50, ma200 = df_raw['Price'], df_raw['SMA50'], df_raw['SMA200']
        h52, l52 = df_raw['price_52_week_high'], df_raw['price_52_week_low']
        df_raw['52W_High_Pct'] = np.where(h52 > 0, (p - h52) / h52, -1)
        df_raw['52W_Low_Pct'] = np.where(l52 > 0, (p - l52) / l52, 0)
        
        c2 = (p > ma50) & (ma50 > ma200) & (df_raw['52W_Low_Pct'] >= 0.25) & (df_raw['52W_High_Pct'] >= -0.25)
        c4 = (p < ma50) & (ma50 < ma200)
        df_raw['Weinstein_Stage'] = np.select([c2, c4], ['Stage 2 🚀 Adv', 'Stage 4 📉 Dec'], default='Stage 1/3')

        # 4. IBD & Group Ranking Load
        df_ibd = pd.DataFrame()
        ibd_p = find_file_robust(DATA_DIR, "IBD.csv")
        if ibd_p:
            try:
                try: df_ibd = pd.read_csv(ibd_p, encoding='utf-8-sig')
                except: df_ibd = pd.read_csv(ibd_p, encoding='cp1252')
                df_ibd.columns = df_ibd.columns.str.strip()
                for c in ['RS Rating', 'Comp. Rating', 'EPS Rating', 'Industry Group Rank']:
                    if c in df_ibd.columns: df_ibd[c] = pd.to_numeric(df_ibd[c].astype(str).str.replace('%','').str.replace(',',''), errors='coerce')
                debug_log.append("✅ קובץ IBD נטען בהצלחה")
            except Exception as e: debug_log.append(f"❌ שגיאת IBD: {e}")

        group_p = find_file_robust(DATA_DIR, "Group Ranking.csv")
        group_df = pd.DataFrame()
        if group_p:
            try:
                try: gdf = pd.read_csv(group_p, encoding='utf-8-sig')
                except: gdf = pd.read_csv(group_p, encoding='cp1252')
                gdf.columns = gdf.columns.str.strip()
                rd = {}
                for c in gdf.columns:
                    cl = c.lower()
                    if 'this wk' in cl or cl == 'rank': rd[c] = 'Rank this Wk'
                    elif '3 wks' in cl: rd[c] = '3 Wks ago'
                    elif 'industry' in cl or 'name' in cl: rd[c] = 'Industry Group Name'
                group_df = gdf.rename(columns=rd)
                if 'Rank this Wk' in group_df.columns and '3 Wks ago' in group_df.columns:
                    group_df['Rank this Wk'] = pd.to_numeric(group_df['Rank this Wk'], errors='coerce')
                    group_df['3 Wks ago'] = pd.to_numeric(group_df['3 Wks ago'], errors='coerce')
                    group_df['Rank_Improvement'] = group_df['3 Wks ago'] - group_df['Rank this Wk']
                debug_log.append("✅ קובץ Group Ranking נטען בהצלחה")
            except Exception as e: debug_log.append(f"❌ שגיאת Group: {e}")

        # Merging
        if not df_ibd.empty and not group_df.empty:
            df_ibd = pd.merge(df_ibd, group_df[['Rank this Wk', 'Rank_Improvement', 'Industry Group Name']], 
                              left_on='Industry Group Rank', right_on='Rank this Wk', how='left')

        if not df_ibd.empty:
            if 'Industry Group Name' in df_raw.columns and 'Industry Group Name' in df_ibd.columns: 
                df_raw = df_raw.drop(columns=['Industry Group Name'])
            icols = ['Symbol', 'RS Rating', 'Comp. Rating', 'EPS Rating', 'Acc/Dis Rating', 'SMR Rating', 
                    'Spon Rating', 'Ind Grp RS', 'Industry Group Rank', 'Rank_Improvement', 'Industry Group Name']
            df_raw = pd.merge(df_raw, df_ibd[[c for c in icols if c in df_ibd.columns]], on='Symbol', how='left')
        
        # RS Backfill
        if 'Perf.Y' in df_raw.columns:
            df_raw['RS Rating'] = df_raw['RS Rating'].fillna(df_raw['Perf.Y'].rank(pct=True)*99).astype(int)

        # Excel Alerts Backfill
        ex_p = glob.glob(os.path.join(DATA_DIR, "Ultimate_Market_V3f_*.xlsx"))
        if ex_p:
            try:
                edfx = pd.read_excel(max(ex_p, key=os.path.getmtime), sheet_name='Full Raw Data')
                df_raw = pd.merge(df_raw, edfx[['Symbol', 'Earnings_Alert', 'Kinetic_Slope', 'VDU_Alert']], on='Symbol', how='left')
            except: pass

        for c in ['Earnings_Alert', 'Kinetic_Slope', 'VDU_Alert']:
            if c not in df_raw.columns: df_raw[c] = ''

        debug_log.append("✅ מיזוג נתונים סופי הושלם")
        return df_raw, group_df, debug_log
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), [f"❌ שגיאה כללית: {e}"]

# ==========================================
# UI Logic
# ==========================================
st.title("📟 HYBRID COMMAND CENTER :: TV + IBD")

df_raw, group_df, debug_log = load_hybrid_data()

# Diagnostic
with st.expander("🔍 מצב אבחון קבצים (X-Ray)"):
    if os.path.exists(DATA_DIR): st.write(f"קבצים בתיקייה: {os.listdir(DATA_DIR)}")
    for log in debug_log: st.write(log)
    if st.button("📡 רענן נתונים ופתח מט
