
import sqlite3
import pandas as pd

def analyze_ma(stock_id: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    對指定股票代號進行 MA 指標分析並評分
    """
    # 1. 抓資料
    query = f"""
    SELECT 日期, 證券代號, 證券名稱, 收盤價
    FROM twse_chip
    WHERE 證券代號 = ?
    ORDER BY 日期
    """
    df = pd.read_sql_query(query, conn, params=(stock_id,))
    df["日期"] = pd.to_datetime(df["日期"], format="%Y%m%d")
    df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")

    # 2. 計算 MA
    df["MA_5"] = df["收盤價"].rolling(window=5).mean()
    df["MA_20"] = df["收盤價"].rolling(window=20).mean()
    df["MA_60"] = df["收盤價"].rolling(window=60).mean()

    # 3. 判斷黃金交叉與多頭排列
    df_valid = df.dropna()
    if df_valid.empty or len(df_valid) < 2:
        return pd.DataFrame([{
            "股票代號": stock_id,
            "股票名稱": "無足夠資料",
            "日期": None,
            "收盤價": None,
            "MA_5": None,
            "MA_20": None,
            "MA_60": None,
            "技術訊號": "資料不足",
            "評分": 0
        }])

    latest = df_valid.iloc[-1]
    prev = df_valid.iloc[-2]

    signals = []

    if latest["MA_5"] > latest["MA_20"] > latest["MA_60"]:
        signals.append("多頭排列")
    if prev["MA_5"] < prev["MA_20"] and latest["MA_5"] > latest["MA_20"]:
        signals.append("黃金交叉")
    if prev["MA_5"] > prev["MA_20"] and latest["MA_5"] < latest["MA_20"]:
        signals.append("死亡交叉")


    # 4. 評分邏輯
    score = 50  # 起始中性分數
    if "多頭排列" in signals:
        score += 30
    if "黃金交叉" in signals:
        score += 20
    if "死亡交叉" in signals:
        score -= 30

    df_result = pd.DataFrame([{
        "股票代號": stock_id,
        "股票名稱": latest["證券名稱"],
        "日期": latest["日期"].date(),
        "收盤價": latest["收盤價"],
        "MA_5": latest["MA_5"],
        "MA_20": latest["MA_20"],
        "MA_60": latest["MA_60"],
        "技術訊號": ", ".join(signals) if signals else "無明顯訊號",
        "評分": max(0, min(100, score))
    }])

    return df_result
