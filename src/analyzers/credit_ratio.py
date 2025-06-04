# credit_ratio.py
#
# 功能：計算一組股票的「5 日平均資卷比分數（0–100）」，
#      採用 arctan 正規化避免超過上限。
# 流程：
#   1. 先對每檔股票算出 avg_ratio（5 日平均資卷比）。
#   2. 取這批股票 avg_ratio 的平均值當做縮放常數 s。
#   3. 用公式 score = (2/π) * arctan(avg_ratio / s) * 100，得到 0–100 分。
#
import sqlite3
import pandas as pd
import numpy as np
import math


def get_avg_ratio(stock_id, db_path="db/stockDB.db"):
    """
    算單一股票的「5 日平均資卷比」。
    資卷比 = 融資今日餘額 ÷ 融券今日餘額（遇除 0 情況同前所述）。
    回傳 float avg_ratio，若找不到資料回 None。
    """
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return None

    # 1. 抓最近 5 個不重複交易日
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT date FROM credit_twse ORDER BY date DESC LIMIT 5",
        conn
    )
    last5 = dates_df["date"].tolist()
    if len(last5) < 1:
        conn.close()
        return None

    # 2. 取這 5 天裡指定股票的 融資今日餘額 + 融券今日餘額
    placeholders = ", ".join(["?"] * len(last5))
    sql = f"""
        SELECT date, 證券代號, 融資今日餘額, 融券今日餘額
        FROM credit_twse
        WHERE date IN ({placeholders})
          AND 證券代號 = ?
    """
    params = last5 + [stock_id]
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()

    if df.empty:
        return None

    # 3. 計算每天的 ratio
    def calc_ratio(row):
        fin = row["融資今日餘額"]
        short = row["融券今日餘額"]
        if short == 0:
            return float(fin) if fin > 0 else 0.0
        else:
            return float(fin) / float(short)

    df["ratio"] = df.apply(calc_ratio, axis=1)

    # 4. 按日期從「舊到新」排序，每天只留一筆
    df = df.sort_values("date")
    df = df.groupby("date").tail(1)

    # 5. 依 last5（最新→最舊）的順序，把 ratio 放到 list（最舊→最新）
    ratio_list = []
    for d in last5[::-1]:
        row = df[df["date"] == d]
        ratio_list.append(row.iloc[0]["ratio"] if not row.empty else 0.0)

    # 6. 算 avg_ratio
    avg_ratio = sum(ratio_list) / len(ratio_list)
    return float(avg_ratio)


def get_credit_scores_arctan(stock_ids, db_path="db/stockDB.db"):
    """
    同時計算一組股票的「打分」(0–100)，用 arctan 公式：
      1. 計算所有 stock_ids 的 avg_ratio，存到 dict avg_dict。
      2. 用 avg_dict 裡不為 None 的那些值，算平均值 s (縮放常數)。
      3. 對每檔：如果 avg_ratio=None → score=None；
         否則 score = (2/π) * arctan(avg_ratio / s) * 100。
    回傳 dict：{ stock_id: score, ... }。
    """
    # 1. 得到每檔股票的 avg_ratio
    avg_dict = {}
    for sid in stock_ids:
        avg_r = get_avg_ratio(sid, db_path)
        avg_dict[sid] = avg_r  # 可能是 float 或 None

    # 2. 只篩出有值的 avg_ratio，計算平均值 s
    valid_ratios = [v for v in avg_dict.values() if v is not None]
    if not valid_ratios:
        # 全部都沒資料
        return {sid: None for sid in stock_ids}

    s = sum(valid_ratios) / len(valid_ratios)
    # 如果 s = 0 (理論上只有所有 avg_ratio 都 = 0)，就把 s 設為 1 以免除以 0
    if s == 0:
        s = 1.0

    # 3. 用 arctan 公式計算每檔分數
    score_dict = {}
    for sid, avg_r in avg_dict.items():
        if avg_r is None:
            score_dict[sid] = None
        else:
            # arctan 正規化：avg_r 越大分數越接近 100，但永遠不會超
            raw = (2 / math.pi) * math.atan(avg_r / s) * 100
            score_dict[sid] = raw  # 已經介於 0–100，不需再 clamp

    return score_dict


if __name__ == "__main__":
    # 測試範例：計算 4 檔股票的分數
    db_path = "db/stockDB.db"
    test_stocks = ["2330", "2303", "2610", "4904"]

    print("=== 測試：arctan 正規化後的 5 日平均資卷比分數 (0–100) ===\n")
    scores = get_credit_scores_arctan(test_stocks, db_path)
    for stock, sc in scores.items():
        if sc is None:
            print(f"{stock}：最近 5 天沒有信用資料，無法計算分數。")
        else:
            print(f"{stock} 的資卷比分數：{sc:.2f}")
