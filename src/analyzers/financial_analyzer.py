#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sqlite3
import pandas as pd
import numpy as np
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class FinancialAnalyzer:
    """
    FinancialAnalyzer 負責：
      1. 保留一個 SQLite 連線
      2. 自動讀取 data/stock_id.csv 得到 stock_id→產業別
      3. 自動計算每個產業的 EPS、營收門檻（第 percentile 百分位）
      4. 計算單檔股票的 EPS 成長率、營收成長率，套用對應產業門檻，轉 0~100 分後平均回傳
    """

    _EPS_LIMIT = 8        # 取最近 8 筆季報
    _REV_LIMIT = 24       # 取最近 24 筆月報

    def __init__(self, db_path: str, stock_csv: str, percentile: float = 75):
        # 1. 建立資料庫連線
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # 2. 讀入 stock_id → 產業別 對照
        self.stock_df = pd.read_csv(stock_csv, dtype=str)
        # 3. 計算各產業門檻
        self.industry_thresholds = self._compute_industry_thresholds(
            percentile)

    def __del__(self):
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except:
            pass

    def _fetch_eps_history(self, stock_id: str) -> Optional[pd.Series]:
        sql = f"""
            SELECT eps
              FROM quarterly_income_statement
             WHERE stock_id = ?
               AND eps IS NOT NULL
             ORDER BY
               CAST(substr(quarter,1,instr(quarter,'.')-1) AS INTEGER) DESC,
               CAST(substr(quarter,instr(quarter,'.')+1,1)   AS INTEGER) DESC
             LIMIT {self._EPS_LIMIT}
        """
        try:
            df = pd.read_sql_query(sql, self.conn, params=(stock_id,))
        except Exception as e:
            logger.warning(f"[ERROR] 拿 {stock_id} EPS 歷史失敗: {e}")
            return None
        if df.shape[0] < self._EPS_LIMIT:
            return None
        ser = df["eps"].astype(float).reset_index(drop=True)
        prev_avg = ser.iloc[4:8].mean()
        if prev_avg <= 0:
            return None
        return ser

    def _fetch_annual_revenue(self, stock_id: str) -> Optional[pd.DataFrame]:
        sql = f"""
            SELECT revenue
              FROM month_revenue
             WHERE stock_id = ?
               AND revenue IS NOT NULL
             ORDER BY ym DESC
             LIMIT {self._REV_LIMIT}
        """
        try:
            df = pd.read_sql_query(sql, self.conn, params=(stock_id,))
        except Exception as e:
            logger.warning(f"[ERROR] 拿 {stock_id} 月營收歷史失敗: {e}")
            return None
        if df.shape[0] < self._REV_LIMIT:
            return None
        rev = df["revenue"].astype(float).reset_index(drop=True)
        prev_total = rev.iloc[12:24].sum()
        if prev_total <= 0:
            return None
        return pd.DataFrame({
            "last_year_revenue": [rev.iloc[0:12].sum()],
            "prev_year_revenue": [prev_total]
        })

    def _compute_industry_thresholds(self, percentile: float) -> dict:
        """
        批次算 出每個產業 的 EPS/REV 百分位門檻
        """
        thresholds = {}
        # group by CSV 內的「產業別」
        for industry, grp in self.stock_df.groupby("產業別"):
            eps_g, rev_g = [], []
            for sid in grp["stock_id"]:
                # EPS 成長率
                eps_ser = self._fetch_eps_history(sid)
                if eps_ser is not None:
                    recent = eps_ser.iloc[0:4].mean()
                    prev = eps_ser.iloc[4:8].mean()
                    eps_g.append((recent - prev) / abs(prev))
                # 營收成長率
                rev_df = self._fetch_annual_revenue(sid)
                if rev_df is not None:
                    last = float(rev_df.at[0, "last_year_revenue"])
                    prev = float(rev_df.at[0, "prev_year_revenue"])
                    rev_g.append((last - prev) / abs(prev))
            # 沒資料就用預設 50% / 30%
            eps_th = np.percentile(eps_g, percentile) if eps_g else 0.5
            rev_th = np.percentile(rev_g, percentile) if rev_g else 0.3
            thresholds[industry] = {
                "eps_threshold": float(eps_th),
                "rev_threshold": float(rev_th)
            }
            logger.info(
                f"產業「{industry}」門檻→ EPS {eps_th:.2f}, REV {rev_th:.2f}")
        return thresholds

    def get_financial_score(self, stock_id: str) -> float:
        # 先找產業
        row = self.stock_df.loc[self.stock_df["stock_id"] == stock_id]
        industry = row.iloc[0]["產業別"] if not row.empty else None
        th = self.industry_thresholds.get(industry, {
            "eps_threshold": 0.5, "rev_threshold": 0.3
        })

        # EPS 分數
        eps_ser = self._fetch_eps_history(stock_id)
        if eps_ser is None:
            eps_score = 0.0
        else:
            recent = eps_ser.iloc[0:4].mean()
            prev = eps_ser.iloc[4:8].mean()
            g = (recent - prev) / abs(prev)
            if g <= 0:
                eps_score = 0.0
            elif g >= th["eps_threshold"]:
                eps_score = 100.0
            else:
                eps_score = (g / th["eps_threshold"]) * 100

        # 營收 分數
        rev_df = self._fetch_annual_revenue(stock_id)
        if rev_df is None:
            rev_score = 0.0
        else:
            last = float(rev_df.at[0, "last_year_revenue"])
            prev = float(rev_df.at[0, "prev_year_revenue"])
            g = (last - prev) / abs(prev)
            if g <= 0:
                rev_score = 0.0
            elif g >= th["rev_threshold"]:
                rev_score = 100.0
            else:
                rev_score = (g / th["rev_threshold"]) * 100

        if eps_ser is None and rev_df is None:
            return 0.0
        return (eps_score + rev_score) / 2


# ────── main execution ──────
if __name__ == "__main__":
    import os
    import pandas as pd

    # 1. 定位到 financial_analyzer.py 所在資料夾
    # .../stock_analysis/src/analyzers
    this_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(
        this_dir, "..", ".."))  # .../stock_analysis

    # 2. 組出 db 與 csv 的絕對路徑
    db_file = os.path.join(project_root, "db",  "stockDB.db")
    csv_file = os.path.join(project_root, "data", "stock_id.csv")

    print(f"DB 路徑：{db_file}")
    print(f"CSV 路徑：{csv_file}")

    # 3. 建立分析器
    analyzer = FinancialAnalyzer(db_file, csv_file)

    # 4. 印出各產業門檻
    print("\n=== 產業門檻 ===")
    for ind, th in analyzer.industry_thresholds.items():
        print(
            f"{ind:10s}  EPS={th['eps_threshold']:.2%}, REV={th['rev_threshold']:.2%}")

    # 5. 讀所有股票、算分、並輸出 CSV
    df_ids = pd.read_csv(csv_file, dtype=str)
    results = []
    for _, r in df_ids.iterrows():
        sid = r["stock_id"]
        ind = r["產業別"]
        score = analyzer.get_financial_score(sid)
        results.append({"stock_id": sid, "industry": ind,
                       "score": round(score, 2)})

    out_dir = os.path.join(project_root, "output")
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "financial_scores.csv")
    pd.DataFrame(results).to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n分數已輸出：{out_csv}")
