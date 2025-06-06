from __future__ import annotations

"""
Credit Score Module (v2)
-----------------------
計算『N 日平均資卷比分數 (0–100)』，支援：
  • days 參數化 (預設 5 日)
  • 支援 TWSE / TPEx (table 參數)
  • 在 SQL 端直接計算 5 日平均 ratio，減少資料傳輸
  • 分數縮放常數 s 可選擇：
      - mean  (簡單平均)
      - median_mad  (中位數 + MAD，對極端值更穩健)
      - quantile  (自訂分位數)

公式：
    score = (2 / π) * arctan(avg_ratio / s) * 100

用法：
    from credit_score_module import get_credit_scores_arctan

    scores = get_credit_scores_arctan(
        ["2330", "2303"],
        db_path="db/stockDB.db",
        table="credit_twse",
        days=10,
        scale_method="median_mad",
        quantile=0.75,
    )

    print(scores["2330"])  # -> 0.0 ~ 100.0 or None
"""

import math
import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Helper(s)
# ---------------------------------------------------------------------------


def _compute_scale(values: pd.Series, method: str, quantile: float) -> float:
    """Return scaling constant *s* given a 1‑D Series of positive values."""

    if method == "mean":
        s = values.mean()
    elif method == "median_mad":
        median = values.median()
        mad = (values - median).abs().median()  # Median Absolute Deviation
        # 避免 mad = 0 → 取 median 作為 s
        s = median + (mad if mad != 0 else 0)
    elif method == "quantile":
        s = values.quantile(quantile)
    else:
        raise ValueError(
            "scale_method must be 'mean', 'median_mad', or 'quantile'"
        )

    return float(s if s != 0 else 1.0)  # 避免 s = 0


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------

def get_credit_scores_arctan(
    stock_ids: List[str],
    db_path: str = "db/stockDB.db",
    *,
    table: str = "credit_twse",      # or "credit_tpex"
    days: int = 5,
    scale_method: str = "mean",      # mean | median_mad | quantile
    quantile: float = 0.75,
) -> Dict[str, Optional[float]]:
    """批次計算 N 日平均資卷比分數 (0–100)。"""

    # 验证输入
    if not stock_ids:
        return {}
    stock_ids = list(dict.fromkeys(stock_ids))  # 去重並保持順序

    # -------------------- 1. 連線 & 最近 N 日 ------------------------------
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return {sid: None for sid in stock_ids}

    dates_df = pd.read_sql_query(
        f"SELECT DISTINCT date FROM {table} ORDER BY date DESC LIMIT ?",
        conn,
        params=(days,),
    )
    last_n = dates_df["date"].tolist()
    if not last_n:
        conn.close()
        return {sid: None for sid in stock_ids}

    # -------------------- 2. SQL 端直接算 AVG(ratio) ------------------------
    placeholders_dates = ", ".join(["?"] * len(last_n))
    placeholders_ids = ", ".join(["?"] * len(stock_ids))

    sql = f"""
        SELECT 證券代號 AS stock_id,
               AVG(
                   CASE
                     WHEN 融券今日餘額 = 0 THEN 融資今日餘額
                     ELSE CAST(融資今日餘額 AS REAL) / 融券今日餘額
                   END
               ) AS avg_ratio
        FROM {table}
        WHERE date IN ({placeholders_dates})
          AND 證券代號 IN ({placeholders_ids})
        GROUP BY 證券代號
    """
    params_sql: List = last_n + stock_ids
    df_avg = pd.read_sql_query(sql, conn, params=params_sql)
    conn.close()

    if df_avg.empty:
        return {sid: None for sid in stock_ids}

    # -------------------- 3. 準備 avg_ratio Series -------------------------
    df_avg["avg_ratio"] = df_avg["avg_ratio"].astype(float)
    series_avg = df_avg.set_index("stock_id")["avg_ratio"]
    # 透過 reindex 把缺失股票補 NaN
    series_full = series_avg.reindex(stock_ids)

    # -------------------- 4. 計算縮放常數 s --------------------------------
    valid_values = series_full.dropna()
    if valid_values.empty:
        return {sid: None for sid in stock_ids}

    s = _compute_scale(valid_values, scale_method, quantile)

    # -------------------- 5. arctan 正規化 → 分數 --------------------------
    def _score(val: int | np.floating) -> Optional[int]:
        if pd.isna(val):
            return None
        score = (2 / math.pi) * math.atan(val / s) * 100
        return int(score)

    score_dict: Dict[str, Optional[int]] = {
        sid: _score(val) for sid, val in series_full.items()
    }

    return score_dict


# ---------------------------------------------------------------------------
# Quick test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. 讀取 stock_id.csv，假設第一欄就是股票代號
    csv_path = "data/stock_id/stock_id.csv"
    try:
        df_ids = pd.read_csv(csv_path, dtype=str)
        # 取出第一欄、去除前後空白、轉成列表
        all_stocks = df_ids.iloc[:, 0].str.strip().tolist()
    except Exception as e:
        print(f"讀取 {csv_path} 失敗：{e}")

    if not all_stocks:
        print(f"{csv_path} 裡沒有任何股票代號，或讀取失敗。")

    # 2. 呼叫 get_credit_scores_arctan，傳入所有股票清單
    scores = get_credit_scores_arctan(
        all_stocks,
        db_path="db/stockDB.db",
        table="credit_twse",    # 或 "credit_tpex"，視資料表而定
        days=5,                 # 你要算「最近 5 日平均」，可改成 10、20
        scale_method="median_mad",  # 或 "mean"、"quantile"
        quantile=0.75           # 只有在 scale_method="quantile" 時才會用到
    )

    # 3. 列印最前面幾筆，或全部都印
    print("=== 全市場 5 日平均資卷比分數 (0–100) ===")
    for sid in all_stocks:
        score = scores.get(sid)
        if score is None:
            pass
        # print(f"{sid}：沒有足夠資料，無法計算。")
        else:
            print(f"{sid}：{score:.0f}")
