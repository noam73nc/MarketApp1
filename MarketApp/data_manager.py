# data_manager.py
import pandas as pd
import json
import os
import io
from datetime import datetime

# מציאת התיקייה שבה יושב הקובץ הזה, וחיפוש תיקיית data בתוכה
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

MANIFEST_PATH = os.path.join(DATA_DIR, "manifest.json")
MARKET_SNAPSHOT_PATH = os.path.join(DATA_DIR, "market_snapshot.pkl")
GROUP_SNAPSHOT_PATH = os.path.join(DATA_DIR, "group_snapshot.pkl")

def get_manifest() -> dict:
    if not os.path.exists(MANIFEST_PATH):
        return {"status": "error", "message": "Manifest not found. Backend might not have run yet."}
    try:
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"status": "error", "message": f"Failed to read manifest: {str(e)}"}

def load_market_data() -> pd.DataFrame:
    try:
        if os.path.exists(MARKET_SNAPSHOT_PATH):
            return pd.read_pickle(MARKET_SNAPSHOT_PATH)
    except Exception as e:
        print(f"Error loading market data: {e}")
    return pd.DataFrame()

def load_group_data() -> pd.DataFrame:
    try:
        if os.path.exists(GROUP_SNAPSHOT_PATH):
            return pd.read_pickle(GROUP_SNAPSHOT_PATH)
    except Exception as e:
        print(f"Error loading group data: {e}")
    return pd.DataFrame()

def get_ui_data():
    manifest = get_manifest()
    df_market = load_market_data()
    df_group = load_group_data()
    return df_market, df_group, manifest

# ==========================================
# פונקציות ייצוא (Export) - עם קישורים אמיתיים!
# ==========================================
def export_to_excel(df):
    output = io.BytesIO()
    df_export = df.copy()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Action Grid')
        
        workbook  = writer.book
        worksheet = writer.sheets['Action Grid']
        
        # הגדרת עיצוב לקישור (כחול וקו תחתון)
        link_format = workbook.add_format({'font_color': 'blue', 'underline': 1})
        
        # מחפש את עמודת TV_Link וכותב אליה קישורים טבעיים של אקסל
        if 'TV_Link' in df_export.columns:
            col_idx = df_export.columns.get_loc('TV_Link')
            worksheet.set_column(col_idx, col_idx, 15) # מרחיב את העמודה
            
            for row_idx, url in enumerate(df_export['TV_Link']):
                if pd.notna(url) and isinstance(url, str) and "symbol=" in url:
                    # מחלץ את הטיקר מתוך הלינק כדי שיוצג בצורה נקייה
                    ticker = url.split("symbol=")[-1]
                    worksheet.write_url(row_idx + 1, col_idx, url, link_format, string=ticker)
                
        # התאמת רוחב לשאר העמודות
        for i, col in enumerate(df_export.columns):
            if col != 'TV_Link':
                worksheet.set_column(i, i, 12)

    return output.getvalue()

# ==========================================
# הכנה ל-LLM (AI Tooling) בעתיד
# ==========================================
def llm_get_top_stocks(min_score: int = 80, limit: int = 10) -> list:
    df = load_market_data()
    if df.empty or 'Action_Score' not in df.columns:
        return []
    top_df = df[df['Action_Score'] >= min_score].sort_values('Action_Score', ascending=False).head(limit)
    return top_df[['Symbol', 'Price', 'Industry Group Name', 'Action_Score', 'Pattern_Badges']].to_dict('records')
