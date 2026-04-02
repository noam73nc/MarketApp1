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
# 📁 הגדרת נתיב נתונים (מותאם לענן ולמקומי)
# ==========================================
DATA_DIR = "data"

# פונקציית עזר למציאת קובץ בתיקייה ללא רגישות לאותיות גדולות/קטנות
def find_file_robust(directory, filename_target):
    if not os.path.exists(directory):
        return None
    files = os.listdir(directory)
    for f in files:
        if f.lower() == filename_target.lower():
            return os.path.join(directory, f)
    return None

# ==========================================
# ⚙️ הגדרות עמוד ותצורה - Terminal Mode
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
    .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; direction: rtl; max-width: 95%; }
    h1, h2, h3 { color: #E6EDF3; font-family: 'Consolas', 'Courier New', monospace; text-transform: uppercase; letter-spacing: 1px; }
    h1 { border-bottom: 2px solid #238636; padding-bottom: 10px; }
    .stDataFrame { direction: ltr; }
    div[data-baseweb="input"] { background-color: #161B22; border: 1px solid #30363D; border-radius: 4px; }
    div[data-baseweb="select"] > div { background-color: #161B22; border: 1px solid #30363D; }
    div[data-testid="stMetricValue"] { color: #2EA043; font-weight: bold; }
    .counter-badge { color: #2EA043; font-weight: bold; font-size: 0.9em; }
    .stDownloadButton > button { background-color: #238636; color: white; border: none; width: 100%; }
    .stDownloadButton > button:hover { background-color: #2EA043; border: none; }
    /* עיצוב כפתור הרענון */
    .refresh-btn > button { background-color: #1F6FEB; color: white; border: 1px solid #388BFD; font-weight: bold; }
    .refresh-btn > button:hover { background-color: #388BFD; border-color: #58A6FF; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 📡 טעינת נתונים משולבת + מנוע תבניות חי (LIVE PATTERNS)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def load_hybrid_data():
    try:
        # --- 1. משיכת נתונים חיים מ-TV ---
        query = (Query()
                 .set_markets('america')
                 .select('name', 'close', 'open', 'high', 'low', 'volume', 'average_volume_10d_calc', 
                         'market_cap_basic', 'sector', 'industry', 
                         'SMA10', 'SMA20', 'SMA50', 'SMA200', 'price_52_week_high', 'price_52_week_low',
                         'Perf.W', 'Perf.1M', 'Perf.3M', 'Perf.Y', 'ATR')
                 .where(
                     Column('close') > 1, 
                     Column('average_volume_10d_calc') > 100000 
                 )
                 .limit(4500)) 
        
        count, df_tv = query.get_scanner_data()
        if df_tv.empty: return pd.DataFrame(), pd.DataFrame()

        rename_map = {
            'ticker': 'Symbol', 'name': 'Company_Name', 'close': 'Price',
            'volume': 'TV_Volume', 'average_volume_10d_calc': 'TV_AvgVol10',
            'market_cap_basic': 'Market Cap', 'industry': 'Industry Group Name'
        }
        df_raw = df_tv.rename(columns=rename_map).copy()
        df_raw['Symbol'] = df_raw['Symbol'].apply(lambda x: x.split(':')[-1] if isinstance(x, str) and ':' in x else x)
        df_raw['TV_Link'] = "https://www.tradingview.com/chart/?symbol=" + df_raw['Symbol']
        df_raw['Market_Cap_B'] = pd.to_numeric(df_raw['Market Cap'], errors='coerce') / 1_000_000_000.0
        df_raw['Dollar_Volume_M'] = (df_raw['Price'] * df_raw['TV_AvgVol10']) / 1_000_000.0

        # --- 2. חישוב תבניות טכניות (LIVE PATTERN ENGINE) ---
        df_raw['Rel_Volume'] = df_raw['TV_Volume'] / df_raw['TV_AvgVol10']
        df_raw['Spread'] = df_raw['high'] - df_raw['low']
        df_raw['Close_Pos'] = np.where(df_raw['Spread'] > 0, (df_raw['Price'] - df_raw['low']) / df_raw['Spread'], 0.5)
        df_raw['ADR_Pct'] = np.where(df_raw['low'] > 0, (df_raw['ATR'] / df_raw['low']) * 100, 0)

        def generate_live_patterns(row):
            badges = []
            price, op, hi, lo = row.get('Price', 0), row.get('open', 0), row.get('high', 0), row.get('low', 0)
            rvol, atr, adr = row.get('Rel_Volume', 1), row.get('ATR', 0), row.get('ADR_Pct', 0)
            sma10, sma20, sma50, sma200 = row.get('SMA10', 0), row.get('SMA20', 0), row.get('SMA50', 0), row.get('SMA200', 0)
            spread, close_pos = row.get('Spread', 0), row.get('Close_Pos', 0.5)
            
            perf_3m = row.get('Perf.3M', 0)
            perf_3m = (perf_3m / 100.0) if not pd.isna(perf_3m) else 0

            hi52 = row.get('price_52_week_high', 0)
            hi52_pct = (price - hi52) / hi52 if hi52 and hi52 > 0 else -99

            if price <= 0: return ""

            if sma50 > 0 and lo < sma50 < price: badges.append("U&R(50) 🛡️")
            if sma20 > 0 and lo < sma20 < price: badges.append("U&R(21) 🛡️")
            if sma50 > 0 and (0.0 <= (price - sma50) / sma50 <= 0.03) and price > sma200: badges.append("Bounce50 🏀")
            if sma20 > 0 and sma50 > 0 and (0.0 <= (price - sma20) / sma20 <= 0.035) and sma20 > sma50: badges.append("Ride20 🏄")
            if rvol > 1.5 and price > op and close_pos > 0.7: badges.append("HVC 🚀")
            if rvol > 1.2 and adr > 0 and (spread / lo * 100 if lo > 0 else 0) > adr and close_pos < 0.4: badges.append("SQUAT 🏋️")
            if atr > 0 and spread < (atr * 0.7) and close_pos > 0.5: badges.append("ID 🕯️")
            if perf_3m > 0.90 and hi52_pct >= -0.20: badges.append("HTF 🚩")
            if adr > 0 and (spread / lo * 100 if lo > 0 else adr) < (adr * 0.6) and rvol < 1.0: badges.append("Tight/VCP 🤏")
            if hi52_pct >= -0.02: badges.append("52W High 👑")
            if sma10 > 0 and (price / sma10 - 1) > 0.15: badges.append("EXT ⚠️")
            
            move_pct = spread / lo * 100 if lo > 0 else 0
            adr_ratio = move_pct / adr if adr > 0 else 0
            if 0.8 <= adr_ratio <= 1.2: badges.append("1 ADR 📏")
            elif 1.5 <= adr_ratio <= 2.5: badges.append("2 ADR 🔥")

            return "  ".join(badges)

        df_raw['Pattern_Badges'] = df_raw.apply(generate_live_patterns, axis=1)

        # --- 3. זיהוי Weinstein Stages ---
        p, ma50, ma200 = df_raw['Price'], df_raw['SMA50'], df_raw['SMA200']
        hi52, lo52 = df_raw['price_52_week_high'], df_raw['price_52_week_low']
        df_raw['52W_High_Pct'] = np.where(hi52 > 0, (p - hi52) / hi52, -99)
        df_raw['52W_Low_Pct'] = np.where(lo52 > 0, (p - lo52) / lo52, 0)
        
        cond_stage2 = (p > ma50) & (ma50 > ma200) & (df_raw['52W_Low_Pct'] >= 0.25) & (df_raw['52W_High_Pct'] >= -0.25)
        cond_stage4 = (p < ma50) & (ma50 < ma200) & (df_raw['52W_High_Pct'] < -0.25)
        cond_stage3 = (~cond_stage2) & (~cond_stage4) & (df_raw['52W_High_Pct'] >= -0.20) & (p < ma50)
        cond_stage1 = (~cond_stage2) & (~cond_stage4) & (~cond_stage3) & (df_raw['52W_High_Pct'] < -0.20) & (abs(p - ma200)/ma200 < 0.10)

        df_raw['Weinstein_Stage'] = np.select(
            [cond_stage2, cond_stage3, cond_stage4, cond_stage1],
            ['Stage 2 🚀 Adv', 'Stage 3 ⚠️ Top', 'Stage 4 📉 Dec', 'Stage 1 🔵 Base'],
            default='N/A'
        )

        # --- 4. שילוב קובץ IBD ---
        df_ibd = pd.DataFrame()
        ibd_path = find_file_robust(DATA_DIR, "IBD.csv")
        
        if ibd_path and os.path.exists(ibd_path):
            df_ibd = pd.read_csv(ibd_path, encoding='utf-8-sig')
            df_ibd.columns = df_ibd.columns.str.strip()
            for c in ['RS Rating', 'Comp. Rating', 'EPS Rating', 'Industry Group Rank']:
                if c in df_ibd.columns:
                    df_ibd[c] = pd.to_numeric(df_ibd[c].astype(str).str.replace('%','').str.replace(',',''), errors='coerce')

        # --- 5. שילוב קובץ Group Ranking ---
        group_path = find_file_robust(DATA_DIR, "Group Ranking.csv")
        group_df = pd.DataFrame()
        if group_path and os.path.exists(group_path):
            group_df = pd.read_csv(group_path, encoding='utf-8-sig')
            group_df.columns = group_df.columns.str.strip()
            cols = list(group_df.columns)
            if len(cols) >= 2:
                cols[0], cols[1] = 'Rank this Wk', '3 Wks ago'
                for i in range(2, len(cols)):
                    if 'Composite' in str(cols[i]): cols[i] = 'Group Composite Rating'
                    elif 'Industry' in str(cols[i]) or 'Name' in str(cols[i]): cols[i] = 'Industry Group Name'
                group_df.columns = cols
            
            group_df['Rank this Wk'] = pd.to_numeric(group_df['Rank this Wk'], errors='coerce')
            group_df['3 Wks ago'] = pd.to_numeric(group_df['3 Wks ago'], errors='coerce')
            group_df['Rank_Improvement'] = group_df['3 Wks ago'] - group_df['Rank this Wk']

        # --- מיזוג נתונים ---
        if not df_ibd.empty and not group_df.empty:
            df_ibd = pd.merge(df_ibd, group_df[['Rank this Wk', 'Rank_Improvement', 'Industry Group Name']], 
                              left_on='Industry Group Rank', right_on='Rank this Wk', how='left')

        if not df_ibd.empty:
            if 'Industry Group Name' in df_raw.columns and 'Industry Group Name' in df_ibd.columns:
                df_raw = df_raw.drop(columns=['Industry Group Name']) 
            
            ibd_cols = ['Symbol', 'RS Rating', 'Comp. Rating', 'EPS Rating', 'Acc/Dis Rating', 'SMR Rating', 
                        'Spon Rating', 'Ind Grp RS', 'Industry Group Rank', 'Rank_Improvement', 'Industry Group Name']
            ibd_cols = [c for c in ibd_cols if c in df_ibd.columns]
            df_raw = pd.merge(df_raw, df_ibd[ibd_cols], on='Symbol', how='left')
        else:
            for c in ['RS Rating', 'Comp. Rating', 'EPS Rating', 'Acc/Dis Rating', 'SMR Rating', 'Spon Rating', 'Ind Grp RS', 'Industry Group Rank', 'Rank_Improvement']:
                df_raw[c] = np.nan

        if 'Perf.Y' in df_raw.columns:
            smart_rs = df_raw['Perf.Y'].rank(pct=True) * 99
            df_raw['RS Rating'] = df_raw['RS Rating'].fillna(smart_rs).astype(int)

        # --- 6. משיכת נתוני אקסל (Earnings/Kinetic) ---
        excel_pattern = os.path.join(DATA_DIR, "Ultimate_Market_V3f_*.xlsx")
        excel_files = glob.glob(excel_pattern)
        if excel_files:
            latest_excel = max(excel_files, key=os.path.getmtime)
            try:
                df_excel = pd.read_excel(latest_excel, sheet_name='Full Raw Data')
                excel_cols = ['Symbol', 'Earnings_Alert', 'Kinetic_Slope', 'VDU_Alert']
                excel_cols = [c for c in excel_cols if c in df_excel.columns]
                df_raw = pd.merge(df_raw, df_excel[excel_cols], on='Symbol', how='left')
            except: pass
        
        for c in ['Earnings_Alert', 'Kinetic_Slope', 'VDU_Alert']:
            if c not in df_raw.columns: df_raw[c] = ''

        return df_raw, group_df

    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame(), pd.DataFrame()

# ==========================================
# רענון ותצוגה
# ==========================================
col_t, col_b = st.columns([5, 1])
with col_t: st.title("📟 HYBRID COMMAND CENTER :: TV + IBD")
with col_b:
    if st.button("📡 רענן נתונים", use_container_width=True):
        load_hybrid_data.clear()
        st.rerun()

# --- בדיקת תקינות תיקייה (Debug) ---
if not os.path.exists(DATA_DIR):
    st.warning(f"⚠️ תיקיית '{DATA_DIR}' לא נמצאה בשורש הפרויקט בגיטהאב.")
else:
    files_found = os.listdir(DATA_DIR)
    if "Group Ranking.csv" not in [f for f in files_found]:
        with st.expander("🔍 מידע טכני - קבצים בתיקייה"):
            st.write(f"נמצאו בתיקיית {DATA_DIR}:", files_found)

df_raw, group_df = load_hybrid_data()

if not df_raw.empty:
    # --- פאנל שליטה ---
    st.markdown("### ⚙️ CORE PARAMETERS")
    c1, c2, c3, c4 = st.columns(4)
    with c1: min_rs = st.number_input("⚡ RS Rating", 1, 99, 85)
    with c2: min_vol = st.number_input("💵 $ Vol (M)", 0.0, 500.0, 5.0)
    with c3: req_lc = st.toggle("> $1B Cap", False)
    with c4:
        stages = [s for s in df_raw['Weinstein_Stage'].unique() if s != '']
        stg_filt = st.multiselect("📊 Stage", stages, default=["Stage 2 🚀 Adv"] if "Stage 2 🚀 Adv" in stages else None)

    mask = (df_raw['RS Rating'] >= min_rs) & (df_raw['Dollar_Volume_M'] >= min_vol)
    if req_lc: mask &= (df_raw['Market_Cap_B'] >= 1.0)
    df_filtered = df_raw[mask]
    if stg_filt: df_filtered = df_filtered[df_filtered['Weinstein_Stage'].isin(stg_filt)]

    # --- פילטרים מתקדמים ---
    with st.expander("🛠️ ADVANCED FILTERS"):
        a1, a2, a3 = st.columns(3)
        with a1:
            if 'Pattern_Badges' in df_raw.columns:
                badges = sorted(list(set(df_raw['Pattern_Badges'].str.split('  ').explode().dropna())))
                b_filt = st.multiselect("Patterns", [b for b in badges if b])
                for b in b_filt: df_filtered = df_filtered[df_filtered['Pattern_Badges'].str.contains(b, na=False)]
        with a2: min_comp = st.number_input("Min Comp", 1, 99, 1)
        with a3: min_eps = st.number_input("Min EPS", 1, 99, 1)
        if min_comp > 1: df_filtered = df_filtered[df_filtered['Comp. Rating'] >= min_comp]
        if min_eps > 1: df_filtered = df_filtered[df_filtered['EPS Rating'] >= min_eps]

    # --- טבלת Strike Zone ---
    st.markdown(f"### 🎯 ACTION GRID ({len(df_filtered)} STOCKS)")
    df_filtered['Action_Score'] = (df_filtered['RS Rating'] / 10) + (pd.to_numeric(df_filtered['Kinetic_Slope'], errors='coerce').fillna(0) / 50).clip(upper=3)
    
    st.dataframe(df_filtered.sort_values('Action_Score', ascending=False), use_container_width=True, hide_index=True, height=400,
        column_config={
            "TV_Link": st.column_config.LinkColumn("SYM 🔗", display_text=r"symbol=(.*)"),
            "RS Rating": st.column_config.ProgressColumn("RS", min_value=0, max_value=99),
            "Price": st.column_config.NumberColumn("PRICE", format="$%.2f")
        })

    # --- גרף אינטראקטיבי ---
    st.markdown("---")
    tickers = sorted(df_filtered['Symbol'].unique())
    if tickers:
        sel_t = st.selectbox("🎯 בחר מניה לגרף:", tickers)
        with st.spinner("טוען..."):
            td = yf.download(sel_t, period="1y", interval="1d", progress=False)
            if not td.empty:
                if isinstance(td.columns, pd.MultiIndex): td.columns = td.columns.get_level_values(0)
                td['SMA21'], td['SMA50'], td['SMA200'] = td['Close'].rolling(21).mean(), td['Close'].rolling(50).mean(), td['Close'].rolling(200).mean()
                disp = td.tail(130)
                cands, vols, s21, s50, s200 = [], [], [], [], []
                for d, r in disp.iterrows():
                    ts = d.strftime('%Y-%m-%d')
                    cands.append({"time": ts, "open": r['Open'], "high": r['High'], "low": r['Low'], "close": r['Close']})
                    vols.append({"time": ts, "value": r['Volume'], "color": '#26a69a80' if r['Close'] >= r['Open'] else '#ef535080'})
                    if pd.notna(r['SMA21']): s21.append({"time": ts, "value": r['SMA21']})
                    if pd.notna(r['SMA50']): s50.append({"time": ts, "value": r['SMA50']})
                    if pd.notna(r['SMA200']): s200.append({"time": ts, "value": r['SMA200']})

                opts = {"width": 1400, "height": 800, "layout": {"textColor": 'white', "background": {"type": 'solid', "color": '#0E1117'}},
                        "watermark": {"visible": True, "fontSize": 140, "text": sel_t, "color": 'rgba(255,255,255,0.06)'},
                        "rightPriceScale": {"scaleMargins": {"top": 0.05, "bottom": 0.25}},
                        "leftPriceScale": {"visible": False, "scaleMargins": {"top": 0.8, "bottom": 0}}}
                
                renderLightweightCharts([{"chart": opts, "series": [
                    {"type": 'Candlestick', "data": cands}, {"type": 'Histogram', "data": vols, "options": {"priceScaleId": 'left'}},
                    {"type": 'Line', "data": s21, "options": {"color": "#1053e6", "title": 'SMA 21'}},
                    {"type": 'Line', "data": s50, "options": {"color": "#14b11c", "title": 'SMA 50'}},
                    {"type": 'Line', "data": s200, "options": {"color": '#d50000', "title": 'SMA 200'}}
                ]}], 'chart')

    # --- Sector Velocity (כעת מופיע בסוף) ---
    st.markdown("---")
    st.markdown("### 🌊 MACRO: SECTOR VELOCITY")
    m1, m2 = st.columns(2)
    with m1:
        if not group_df.empty:
            st.caption("🏆 LEADERS: TOP 40 IBD GROUPS")
            st.dataframe(group_df.sort_values('Rank this Wk').head(40), use_container_width=True, hide_index=True)
    with m2:
        if not group_df.empty:
            top_j = group_df.sort_values('Rank_Improvement', ascending=False).head(20)
            st.caption("🚀 MOMENTUM: JUMPING GROUPS")
            j_df = df_raw[df_raw['Industry Group Name'].isin(top_j['Industry Group Name'])]
            st.dataframe(j_df[['Industry Group Name', 'Rank_Improvement', 'TV_Link', 'RS Rating']].sort_values(['Rank_Improvement', 'RS Rating'], ascending=False), 
                         use_container_width=True, hide_index=True)
