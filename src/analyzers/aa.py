import os
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
      4. 根據 industry_PER.csv 與 stock_id.csv，把股票本益比 vs. 產業平均本益比做公平比較，給 0~100 分
      5. EPS、營收、PER 三項分數平均 → 最終財務面分數
    """

    _EPS_LIMIT = 8
    _REV_LIMIT = 24

    def __init__(self, db_path: str,
                 stock_info_path: Optional[str] = None,
                 industry_per_path: Optional[str] = None):
        # 初始化資料庫連線
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)

        # 預設 data 路徑
        base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        if stock_info_path is None:
            stock_info_path = os.path.join(
                base, "data", "stock_id", "stock_id.csv")
        if industry_per_path is None:
            industry_per_path = os.path.join(base, "data", "industry_PER.csv")

        # 載入股票 → 產業對應
        try:
            self.stock_info = pd.read_csv(
                stock_info_path,
                dtype={"stock_id": str},
                encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"[WARN] 載入 stock_id.csv 失敗: {e}")
            self.stock_info = pd.DataFrame(columns=["stock_id", "產業別"])

        # 載入產業平均本益比
        try:
            self.industry_per = pd.read_csv(
                industry_per_path,
                encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"[WARN] 載入 industry_PER.csv 失敗: {e}")
            self.industry_per = pd.DataFrame(columns=["產業別", "平均本益比"])

    def __del__(self):
        try:
            if hasattr(self, "conn"):
                self.conn.close()
        except Exception:
            pass

    def _fetch_eps_history(self, stock_id: str) -> Optional[pd.Series]:
        """
        從 quarterly_income_statement 撈最近 8 筆 EPS，
        若不到 8 筆或前 4 季平均 ≤ 0，回 None。
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
            return None

        return eps_vals

    def _fetch_annual_revenue(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從 month_revenue 撈最近 24 個月營收，
        若不到 24 筆或前 12 月總營收 ≤ 0，回 None。
        回傳 DataFrame，columns=['last_year_revenue','prev_year_revenue']。
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
            logger.warning(f"[ERROR] 拿 {stock_id} 營收歷史失敗: {e}")
            return None

        if df.shape[0] < self._REV_LIMIT:
            logger.info(
                f"[INFO] {stock_id} 營收筆數 {df.shape[0]} 少於 {self._REV_LIMIT}，跳過")
            return None

        rev_vals = df["revenue"].astype(float).reset_index(drop=True)
        last_year_total = rev_vals.iloc[0:12].sum()
        prev_year_total = rev_vals.iloc[12:24].sum()
        if prev_year_total <= 0:
            return None

        return pd.DataFrame({
            "last_year_revenue": [last_year_total],
            "prev_year_revenue": [prev_year_total]
        })

    def _fetch_latest_per(self, stock_id: str) -> Optional[float]:
        """
        抓最新本益比
        """
        for table in ("twse_price", "tpex_price"):
            sql = f"""
                SELECT 本益比
                FROM {table}
                WHERE 證券代號 = ?
                  AND 本益比 IS NOT NULL
                ORDER BY 日期 DESC
                LIMIT 1
            """
            try:
                df = pd.read_sql_query(sql, self.conn, params=(stock_id,))
                if not df.empty:
                    val = df.iloc[0, 0]
                    if pd.notna(val):
                        return float(val)
            except Exception:
                continue
        logger.info(f"[INFO] {stock_id} 無本益比資料")
        return None

    def _calculate_per_score(self, stock_id: str) -> Optional[float]:
        """
        本益比 vs. 產業平均本益比，ratio 方法給分
        """
        per = self._fetch_latest_per(stock_id)
        if per is None:
            return None
        row = self.stock_info[self.stock_info["stock_id"] == stock_id]
        if row.empty:
            logger.info(f"[INFO] {stock_id} 無產業別")
            return None
        industry = row.iloc[0]["產業別"]
        avg_row = self.industry_per[self.industry_per["產業別"] == industry]
        if avg_row.empty:
            logger.info(f"[INFO] 產業 {industry} 無平均本益比")
            return None
        try:
            avg_per = float(avg_row.iloc[0]["平均本益比"])
        except Exception:
            logger.info(f"[INFO] 平均本益比格式錯誤")
            return None
        if avg_per <= 0:
            return None
        ratio = per / avg_per
        score = min(ratio, 1/ratio) * 100
        return score

    def get_financial_score(self, stock_id: str) -> float:
        """
        計算財務面分數：EPS、營收、PER 三項平均
        """
        # EPS 分數
        eps_series = self._fetch_eps_history(stock_id)
        if eps_series is None:
            eps_score = 0.0
            logger.info(f"[DEBUG] {stock_id} EPS score = 0")
        else:
            recent_avg = eps_series.iloc[0:4].mean()
            prev_avg = eps_series.iloc[4:8].mean()
            growth = (recent_avg - prev_avg) / \
                abs(prev_avg) if prev_avg != 0 else 0
            if growth <= 0:
                eps_score = 0.0
            elif growth >= 0.5:
                eps_score = 100.0
            else:
                eps_score = (growth / 0.5) * 100
            logger.debug(f"[DEBUG] {stock_id} EPS score = {eps_score:.2f}")

        # 營收分數
        rev_df = self._fetch_annual_revenue(stock_id)
        if rev_df is None:
            rev_score = 0.0
            logger.info(f"[DEBUG] {stock_id} Revenue score = 0")
        else:
            last_rev = float(rev_df.at[0, "last_year_revenue"])
            prev_rev = float(rev_df.at[0, "prev_year_revenue"])
            growth = (last_rev - prev_rev) / \
                abs(prev_rev) if prev_rev != 0 else 0
            if growth <= 0:
                rev_score = 0.0
            elif growth >= 0.3:
                rev_score = 100.0
            else:
                rev_score = (growth / 0.3) * 100
            logger.debug(f"[DEBUG] {stock_id} Revenue score = {rev_score:.2f}")

        # PER 分數
        per_score = self._calculate_per_score(stock_id)

        # 組合分數
        eps_s = eps_score
        rev_s = rev_score
        if per_score is None:
            return (eps_s + rev_s) / 2
        return (eps_s + rev_s + per_score) / 3


# --------- 測試範例 ----------
if __name__ == "__main__":
    import os

    base = os.path.dirname(__file__)
    db_file = os.path.abspath(os.path.join(
        base, "..", "..", "db", "stockDB.db"))
    analyzer = FinancialAnalyzer(db_file)
    stock_ids = ["2330", "2454", "3030", "2610", "4904", "2303"]
    for sid in stock_ids:
        score = analyzer.get_financial_score(sid)
        print(f"股票 {sid} 的財務面分數：{score:.2f}")
