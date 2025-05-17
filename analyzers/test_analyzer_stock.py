from swing_trading_analysis import analyze_swing_stock, DB_PATH
import sqlite3
import pandas as pd

VOLUME_THRESHOLD = 100_000
TOP_N = 30

def get_top30_swing_stocks() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        stock_ids = pd.read_sql_query(
            "SELECT DISTINCT 證券代號 FROM twse_chip", conn
        )['證券代號'].tolist()

        results = []
        for sid in stock_ids:
            df = analyze_swing_stock(sid)
            if df.empty or df.iloc[0]['日期'] is None:
                continue

            latest = df.iloc[0].copy()
            date_str = latest['日期'].strftime('%Y%m%d')

            vol_df = pd.read_sql_query(
                "SELECT 成交股數 FROM twse_chip WHERE 證券代號 = ? AND 日期 = ?",
                conn, params=(sid, date_str)
            )
            if not vol_df.empty and int(vol_df.at[0, '成交股數']) > VOLUME_THRESHOLD:
                latest['成交股數'] = int(vol_df.at[0, '成交股數'])
                results.append(latest)

        if results:
            return pd.DataFrame(results).sort_values('評分', ascending=False).head(TOP_N).reset_index(drop=True)
        else:
            return pd.DataFrame()
    finally:
        conn.close()

if __name__ == '__main__':
    df_top30 = get_top30_swing_stocks()
    if not df_top30.empty:
        print(df_top30.to_string(index=False))
    else:
        print("⚠️ 無符合條件的股票")
