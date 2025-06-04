# borrow_module.py
#
# 方案A：借券趨勢分數（方法一：「線性回歸斜率」判斷趨勢）
#
# 提供函式 get_borrow_score(stock_id, db_path)：
#   - stock_id：欲計算分數的股票代號 (字串)
#   - db_path：SQLite 資料庫完整路徑 (字串)
# 回傳：最終借券分數（浮點數，範圍 0–100）。若該股票最近 5 天都無借券資料，回傳 None。
#
# 算法流程：
# 1. 連接到 ticket_twse 表，取最近 5 個交易日日期。
# 2. 把這 5 天所有股票的「借券當日餘額」與「借券限額」抓下來。
# 3. 計算每檔股票每天的「借券利用率」 = 借券當日餘額 ÷ (借券限額 × 1000)。（限額為 0 時，利用率設為 0）
# 4. pivot 成寬表：index=股票代號、columns=日期，values=利用率；空值以 0 補齊。
# 5. 計算「當日利用率分數」：用今天所有股票的利用率做最小–最大正規化，得 0–100。
# 6. 計算「5 日趨勢斜率」：對每檔股票的 5 天利用率做簡單線性回歸，求斜率 slope（利用 numpy.polyfit）。
# 7. 在所有股票的 slope 中做最小–最大正規化，得趨勢分數 0–100。斜率 > 0（借量持續增加）得分低；斜率 < 0（借量下降）得分高。
# 8. 最終分數 = clamp((當日分數 + 趨勢分數) ÷ 2)。
# 如果最近 5 天都沒有資料或發生錯誤，回 None。

import sqlite3
import pandas as pd
import numpy as np


def clamp(val, low=0, high=100):
    """把 val 限縮到 [low, high] 之間。"""
    return max(low, min(high, val))


def get_borrow_score(stock_id, db_path="db/stockDB.db"):
    """
    計算指定股票的「最終借券分數」（0–100），包含：
      - 當日利用率分數 (score_today)
      - 5 日利用率趨勢分數 (trend_score via slope, 斜率向上借量增加得分低；向下借量減少得分高)
      最終分數 = (score_today + trend_score) / 2

    如果最近 5 天沒有借券資料，回傳 None。
    """
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return None

    # 1. 取最近 5 個不重複交易日
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT date FROM ticket_twse ORDER BY date DESC LIMIT 5",
        conn
    )
    last5 = dates_df["date"].tolist()
    # 如果不到 5 天，就沒有完整 5 日數據
    if len(last5) < 5:
        conn.close()
        return None

    # 2. 取出這 5 天所有股票的「借券當日餘額」和「借券限額」
    placeholders = ", ".join(["?"] * len(last5))
    sql = f"""
        SELECT date, 股票代號, 借券當日餘額, 借券限額
        FROM ticket_twse
        WHERE date IN ({placeholders})
    """
    df = pd.read_sql_query(sql, conn, params=last5)
    conn.close()

    if df.empty:
        return None

    # 3. 計算「借券利用率」
    #    實際可借股數 = 借券限額 × 1000；利用率 = 借券當日餘額 ÷ 實際可借股數
    #    如果借券限額為 0，就把利用率設成 0（避免除以 0）。
    df["實際限額"] = df["借券限額"] * 1000
    df["util"] = df.apply(
        lambda r: 0.0 if r["借券限額"] == 0 else r["借券當日餘額"] / r["實際限額"],
        axis=1
    )

    # 4. 轉成寬表：index=股票代號、columns=日期、values=util；空值補 0
    util_pivot = df.pivot(index="股票代號", columns="date",
                          values="util").fillna(0)

    # 如果目標股票不在這張寬表的 index，就回 None
    if stock_id not in util_pivot.index:
        return None

    # 5. 當日利用率分數 (score_today)
    today_date = last5[0]
    today_utils = util_pivot[today_date]  # Series：所有股票今天的利用率
    min_util = today_utils.min()
    max_util = today_utils.max()

    util_today = today_utils[stock_id]
    if max_util == min_util:
        score_today = 100.0
    else:
        score_today = (max_util - util_today) / (max_util - min_util) * 100
    score_today = clamp(score_today)

    # 6. 計算「5 日趨勢斜率」(slope)：
    #    用 np.polyfit 讓 x=[0,1,2,3,4]（最舊→今天），y=這檔股票各天的利用率
    x = np.array(range(5), dtype=float)  # [0, 1, 2, 3, 4]
    # 取出某股票在 5 天的 util，最舊→今天順序
    y_self = np.array([util_pivot.loc[stock_id, d]
                      for d in reversed(last5)], dtype=float)
    # np.polyfit 回傳多項式係數，第一項就是 slope
    slope_self = np.polyfit(x, y_self, 1)[0]

    # 對所有股票都算 slope，才能做 min–max 正規化
    slopes = {}
    for sid in util_pivot.index:
        y_other = np.array([util_pivot.loc[sid, d]
                           for d in reversed(last5)], dtype=float)
        slopes[sid] = np.polyfit(x, y_other, 1)[0]

    all_slopes = np.array(list(slopes.values()), dtype=float)
    min_slope = all_slopes.min()
    max_slope = all_slopes.max()

    # 7. 趨勢分數 (trend_score)：min–max 正規化 slope
    #    斜率越高 (借量持續往上) → 風險越高 → trend_score 越低
    if max_slope == min_slope:
        trend_score = 100.0
    else:
        trend_score = (max_slope - slope_self) / (max_slope - min_slope) * 100
    trend_score = clamp(trend_score)

    # 8. 最終分數 = (score_today + trend_score) / 2
    final_score = clamp((score_today + trend_score) / 2)
    return float(final_score)


if __name__ == "__main__":
    # 簡單的測試主程式：示範如何呼叫 get_borrow_score
    db_path = "db/stockDB.db"
    test_stocks = ["2303", "4904", "2610", "2330"]  # 可自行替換任何股票代號

    print("使用方案A（線性回歸斜率趨勢）計算借券分數：\n")
    for stock in test_stocks:
        score = get_borrow_score(stock, db_path)
        if score is None:
            print(f"{stock}：最近 5 天無借券資料，無法計算分數。")
        else:
            print(f"{stock} 的最終借券分數：{score:.2f}")
