# src/analyzers/financial_analyzer.py

import sqlite3
import pandas as pd
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FinancialAnalyzer:
    """
    FinancialAnalyzer 負責：
      1. 保留一個 SQLite 連線
      2. 從 quarterly_income_statement 撈最近 8 季 EPS，算 EPS 成長率（僅在前 4 季 avg > 0 時）
      3. 從 month_revenue 撈最近 24 個月營收，算營收成長率（僅在前 12 月總營收 > 0 時）
      4. 把 EPS 成長率與營收成長率轉成 0～100 分後取平均作為『財務面分數』
    """

    _EPS_LIMIT = 8        # 取最近 8 筆季報
    _REV_LIMIT = 24       # 取最近 24 筆月報

    def __init__(self, db_path: str):
        """
        初始化 FinancialAnalyzer。

        參數：
          - db_path: SQLite 資料庫路徑，例如 "C:/.../stockDB.db"
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)

    def __del__(self):
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception:
            pass

    def _fetch_eps_history(self, stock_id: str) -> Optional[pd.Series]:
        """
        從 quarterly_income_statement 撈最近 8 筆 EPS，
        並依「年、季」排序（最新在前），回傳 Series (index 0~7 對應最新→最舊的 eps)。
        若不到 8 筆或前 4 季平均 ≤ 0，回 None（跳過成長率計算）。
        """
        sql = f"""
            SELECT eps
            FROM quarterly_income_statement
            WHERE stock_id = ?
              AND eps IS NOT NULL
            ORDER BY
              CAST(substr(quarter, 1, instr(quarter, '.') - 1) AS INTEGER) DESC,
              CAST(substr(quarter, instr(quarter, '.') + 1, 1) AS INTEGER) DESC
            LIMIT {self._EPS_LIMIT}
        """
        try:
            df = pd.read_sql_query(sql, self.conn, params=(stock_id,))
        except Exception as e:
            logger.warning(f"[ERROR] 拿 {stock_id} EPS 歷史失敗: {e}")
            return None

        if df.shape[0] < self._EPS_LIMIT:
            logger.info(
                f"[INFO] {stock_id} EPS 筆數 {df.shape[0]} 少於 {self._EPS_LIMIT}，跳過")
            return None

        eps_vals = df["eps"].astype(float).reset_index(drop=True)
        recent_avg = eps_vals.iloc[0:4].mean()
        prev_avg = eps_vals.iloc[4:8].mean()
        if prev_avg <= 0:
            # 前期平均 EPS ≤ 0，跳過成長率
            return None

        return eps_vals

    def _fetch_annual_revenue(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從 month_revenue 撈最近 24 筆 (最近 24 個月) 的 revenue，
        若不到 24 筆或前 12 月總營收 ≤ 0，回 None（跳過成長率計算）。
        回傳一列 DataFrame，columns=['last_year_revenue','prev_year_revenue']。
        """
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
            logger.info(
                f"[INFO] {stock_id} 營收筆數 {df.shape[0]} 少於 {self._REV_LIMIT}，跳過")
            return None

        rev_vals = df["revenue"].astype(float).reset_index(drop=True)
        last_year_total = rev_vals.iloc[0:12].sum()
        prev_year_total = rev_vals.iloc[12:24].sum()
        if prev_year_total <= 0:
            # 前 12 月總營收 ≤ 0，跳過成長率
            return None

        return pd.DataFrame({
            "last_year_revenue": [last_year_total],
            "prev_year_revenue": [prev_year_total]
        })

    def get_financial_score(self, stock_id: str) -> float:
        """
        計算某檔股票的『財務面分數』 (0~100)，步驟：
          1. 拿最近 8 季 EPS，若前 4 季 avg > 0，計算 EPS 成長率並轉換成 0~100 分
             否則 EPS 分數設為 0。
          2. 拿最近 24 個月營收，若前 12 月總營收 > 0，計算營收成長率並轉換成 0~100 分
             否則營收分數設為 0。
          3. 最終分數 = (EPS 分數 + 營收分數) / 2；若兩者皆跳過，回 0。
        """
        # 1. 處理 EPS
        eps_series = self._fetch_eps_history(stock_id)
        if eps_series is None:
            logger.info(f"[DEBUG] {stock_id} EPS 不符合計算條件 → EPS 分數 = 0")
            eps_score = 0.0
        else:
            recent_avg = eps_series.iloc[0:4].mean()
            prev_avg = eps_series.iloc[4:8].mean()
            growth = (recent_avg - prev_avg) / abs(prev_avg)
            # EPS 成長率規則：≤0→0；≥0.5→100；否則線性映射
            if growth <= 0:
                eps_score = 0.0
            elif growth >= 0.5:
                eps_score = 100.0
            else:
                eps_score = (growth / 0.5) * 100
            logger.debug(
                f"{stock_id} EPS 最近4季平均={recent_avg:.2f}，前4季平均={prev_avg:.2f}，EPS 成長率={growth:.4f}，EPS 分數={eps_score:.2f}")

        # 2. 處理營收
        rev_df = self._fetch_annual_revenue(stock_id)
        if rev_df is None:
            logger.info(f"[DEBUG] {stock_id} 營收不符合計算條件 → 營收分數 = 0")
            rev_score = 0.0
        else:
            last_rev = float(rev_df.at[0, "last_year_revenue"])
            prev_rev = float(rev_df.at[0, "prev_year_revenue"])
            growth = (last_rev - prev_rev) / abs(prev_rev)
            # 營收成長率規則：≤0→0；≥0.3→100；否則線性映射
            if growth <= 0:
                rev_score = 0.0
            elif growth >= 0.3:
                rev_score = 100.0
            else:
                rev_score = (growth / 0.3) * 100
            logger.debug(
                f"{stock_id} 營收 近12月={last_rev:.0f}，前12月={prev_rev:.0f}，營收成長率={growth:.4f}，營收分數={rev_score:.2f}")

        # 3. 最終分數
        if eps_series is None and rev_df is None:
            return 0.0
        return (eps_score + rev_score) / 2


# --------- 測試範例 (直接執行此檔時才跑) ----------
if __name__ == "__main__":
    import os

    current_dir = os.path.dirname(__file__)
    db_file = os.path.abspath(os.path.join(
        current_dir, "..", "..", "db", "stockDB.db"))
    logger.info(f"連到的資料庫路徑: {db_file}")

    analyzer = FinancialAnalyzer(db_file)
    stock_id_list = ["2330", "2454", "3030", "2610", "4904", "2303"]
    for sid in stock_id_list:
        score = analyzer.get_financial_score(sid)
        print(f"股票 {sid} 的財務面分數：{score:.2f}")
