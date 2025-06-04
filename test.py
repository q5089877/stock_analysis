# test_with_twse_chip.py

import sqlite3
import pandas as pd
from src.analyzers.technical_indicator import TechnicalIndicatorCalculator


def main():
    # 1. 先連到 stockDB.db
    db_path = "db/stockDB.db"  # 如果你的資料庫在其他地方，就改成對應路徑
    conn = sqlite3.connect(db_path)

    # 2. 準備要查詢的股票代號
    stock_id = "2330"  # 你要測試的股票代號，例如台積電是 2330

    # 3. 撰寫 SQL，從 twse_chip 取出「日期、收盤、最高、最低」
    #    注意：這裡的日期原本是整數 (e.g. 20250520)，後面會再轉成 pandas 的日期型態
    query = f"""
        SELECT 日期, 收盤價, 最高價, 最低價
        FROM twse_chip
        WHERE 證券代號 = '{stock_id}'
        ORDER BY 日期 ASC
    """

    # 4. 用 pandas 讀 SQL 結果
    df_price = pd.read_sql_query(query, conn)

    # 5. 把「整數格式的日期」轉成真正的 pandas Timestamp
    #    先把 index 設成這個欄位（原本是整數）
    #    然後用 pd.to_datetime( ..., format='%Y%m%d') 把它變成 YYYY-MM-DD
    df_price['日期'] = pd.to_datetime(df_price['日期'], format='%Y%m%d')
    df_price.set_index('日期', inplace=True)

    # 6. 改欄位名稱，對應到 calculate_xxx() 裡面要用的 'close', 'high', 'low'
    df_price.rename(columns={
        '收盤價': 'close',
        '最高價': 'high',
        '最低價': 'low'
    }, inplace=True)

    # 7. 檢查一下最後 5 筆，看看有沒有成功拿到我們需要的「close, high, low」
    print("=== 從 twse_chip 撈到的資料（最後 5 筆） ===")
    print(df_price.tail())
    print("\n--- 分隔線 ---\n")

    # 8. 建立 TechnicalIndicatorCalculator，開始計算
    calc = TechnicalIndicatorCalculator(df_price)

    # 8.1 計算 RSI（最後 5 筆）
    rsi_series = calc.calculate_rsi(period=14)
    print("RSI（最後 5 筆）：")
    print(rsi_series.tail(5))
    print("\n--- 分隔線 ---\n")

    # 8.2 計算 MACD（最後 5 筆）
    macd_df = calc.calculate_macd()
    print("MACD（最後 5 筆）：")
    print(macd_df.tail(5))
    print("\n--- 分隔線 ---\n")

    # 8.3 計算 Bollinger Bands（最後 5 筆）
    boll_df = calc.calculate_bollinger(period=20, num_std=2)
    print("Bollinger Bands（最後 5 筆）：")
    print(boll_df.tail(5))
    print("\n--- 分隔線 ---\n")

    # 8.4 計算 KD（最後 5 筆）
    kd_df = calc.calculate_kd(k_period=9, d_period=3)
    print("KD（最後 5 筆）：")
    print(kd_df.tail(5))
    print("\n--- 分隔線 ---\n")

    # 8.5 一次算所有指標（最後 5 筆）
    all_indicators = calc.run_all()
    print("所有指標合併（最後 5 筆）：")
    print(all_indicators.tail(5))

    # 9. 關閉資料庫連線
    conn.close()


if __name__ == "__main__":
    main()
