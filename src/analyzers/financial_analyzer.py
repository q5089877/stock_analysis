# src/analyzers/financial_analyzer.py

import os
import sqlite3
import pandas as pd
from typing import Optional


class FinancialAnalyzer:
    """
    FinancialAnalyzer 負責：
      1. 從 SQLite 資料庫撈出每季 EPS 與每月營收，
      2. 計算 EPS 成長率與營收成長率，
      3. 再依照成長率轉成 0～100 分，最後取平均作為『財務面分數』。
    """

    def __init__(self, db_path: str):
        """
        初始化 FinancialAnalyzer。

        參數：
          - db_path: str
              SQLite 資料庫檔案路徑，例如 "C:/.../stock_analysis/db/stockDB.db"。
        """
        self.db_path = db_path

    def _debug_list_tables(self):
        """
        列出目前連到的 SQLite 資料庫裡，所有的 table 名稱。
        用來檢查是否有 quarterly_income_statement、month_revenue 等表。
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cur.fetchall()]
            conn.close()
            print("[DEBUG] 目前資料庫裡的表：", tables)
        except Exception as e:
            print(f"[DEBUG] 列出 table 失敗：{e}")

    def _fetch_eps_history(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從 quarterly_income_statement 表撈出最近 8 筆 (最近八季) 的 EPS，
        並依年、季的數字大小排序（真正最新的排在最上）。
        回傳一張只有 ['quarter', 'eps'] 的 DataFrame（共 8 筆）。

        假設 quarterly_income_statement 的結構包含：
          - stock_id   (TEXT)
          - quarter    (TEXT，例如 '114.1Q', '113.4Q' 等格式)
          - eps        (REAL)

        處理：
          1. 用 SQLite 函數先把 quarter 的「年」部分取出來、轉成整數，
             再把「季」部分取出來、轉成整數。
          2. 依照 年 DESC、季 DESC 排序，再 LIMIT 8。

        回傳 DataFrame：
          index: 0~7（共 8 筆），columns: ['quarter', 'eps']，已以 年+季 真正降序排序。
          如果資料不到 8 筆，回傳 None。
        """
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT
                  quarter,
                  eps
                FROM quarterly_income_statement
                WHERE stock_id = ?
                  AND eps IS NOT NULL
                ORDER BY
                  CAST(substr(quarter, 1, instr(quarter, '.') - 1) AS INTEGER) DESC,
                  CAST(substr(quarter, instr(quarter, '.') + 1, 1) AS INTEGER) DESC
                LIMIT 8
            """
            # 說明：
            # - instr(quarter, '.') 會找到 '.' 在 quarter 中的位置，例如 '114.1Q' 的 '.' 在第 4 個字元。
            # - substr(quarter, 1, instr(...) - 1) = '114'（代表年），再 CAST(... AS INTEGER) 轉成 114。
            # - substr(quarter, instr(...) + 1, 1) = '1'（代表第幾季），CAST(... AS INTEGER) 轉成 1。
            df = pd.read_sql_query(query, conn, params=(stock_id,))
            conn.close()
            if df.empty or len(df) < 8:
                return None

            # 已經依年+季排序，直接回傳
            return df.reset_index(drop=True)
        except Exception as e:
            print(f"[ERROR] 抓 EPS 歷史資料失敗: {e}")
            return None

    def _fetch_annual_revenue(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從 month_revenue 表撈出最近 24 筆 (最近 24 個月) 的月營收，
        並回傳一列，包含 ['last_year_revenue', 'prev_year_revenue']。

        假設 month_revenue 的結構包含：
          - stock_id   (TEXT)
          - ym         (TEXT，格式 "YYYY/MM")
          - revenue    (INTEGER 或 REAL)：該月營收

        處理：
          1. 抓最近 24 筆（依 ym DESC 排序）。
          2. 如果少於 24 筆，回 None。
          3. 前 12 筆加總為 last_year_revenue，後 12 筆加總為 prev_year_revenue。

        回傳 DataFrame：一列一欄，col=['last_year_revenue','prev_year_revenue']。
        """
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT ym, revenue
                FROM month_revenue
                WHERE stock_id = ?
                  AND revenue IS NOT NULL
                ORDER BY ym DESC
                LIMIT 24
            """
            df = pd.read_sql_query(query, conn, params=(stock_id,))
            conn.close()
            if df.empty or len(df) < 24:
                return None

            df = df.reset_index(drop=True)
            # 0~11 為最近 12 個月，12~23 為前 12 個月
            last_year_revenue = df.loc[0:11, "revenue"].astype(float).sum()
            prev_year_revenue = df.loc[12:23, "revenue"].astype(float).sum()

            result = pd.DataFrame({
                "last_year_revenue": [last_year_revenue],
                "prev_year_revenue": [prev_year_revenue]
            })
            return result
        except Exception as e:
            print(f"[ERROR] 抓年度營收歷史資料失敗: {e}")
            return None

    def get_financial_score(self, stock_id: str) -> float:
        """
        計算某支股票的『財務面分數』（0~100），步驟如下：
          1. 列出資料庫裡所有表，確認是否有必要的表格。
          2. 撈出最近 8 筆 EPS，計算「最近4季 EPS 平均」與「前4季 EPS 平均」，再算 EPS 成長率。
          3. 撈出最近 24 筆月營收，計算「近 12 月總營收」與「前 12 月總營收」，再算營收成長率。
          4. 把 EPS 成長率與營收成長率，用規則換成 0~100 分後取平均作為最終分數。
          5. 若 EPS 或營收資料不足，對應分數設 0；若兩者都不足，回 0 分。
        """
        # 1. 列出目前資料庫裡有哪些表格
        self._debug_list_tables()

        # 2. EPS 部分
        eps_df = self._fetch_eps_history(stock_id)
        if eps_df is None:
            print(f"[DEBUG] {stock_id} EPS 歷史資料不足，EPS 成長率分數設為 0")
            eps_score = 0.0
        else:
            # 已依「年、季」真正降序排序，0~3 為最近 4 季，4~7 為前 4 季
            recent_four_eps = eps_df.loc[0:3, "eps"].astype(float).mean()
            prev_four_eps = eps_df.loc[4:7, "eps"].astype(float).mean()
            if prev_four_eps <= 0:
                eps_score = 0.0
            else:
                eps_growth = (recent_four_eps - prev_four_eps) / \
                    abs(prev_four_eps)
                if eps_growth <= 0:
                    eps_score = 0.0
                elif eps_growth >= 0.5:
                    eps_score = 100.0
                else:
                    eps_score = eps_growth / 0.5 * 100
            print(
                f"[DEBUG] {stock_id} EPS 平均：最近4季={recent_four_eps:.2f}，前4季={prev_four_eps:.2f} → EPS 成長率分數={eps_score:.2f}")

        # 3. 營收部分
        rev_df = self._fetch_annual_revenue(stock_id)
        if rev_df is None:
            print(f"[DEBUG] {stock_id} 營收歷史資料不足，營收成長率分數設為 0")
            rev_score = 0.0
        else:
            last_revenue = float(rev_df.at[0, "last_year_revenue"])
            prev_revenue = float(rev_df.at[0, "prev_year_revenue"])
            if prev_revenue <= 0:
                rev_score = 0.0
            else:
                rev_growth = (last_revenue - prev_revenue) / abs(prev_revenue)
                if rev_growth <= 0:
                    rev_score = 0.0
                elif rev_growth >= 0.3:
                    rev_score = 100.0
                else:
                    rev_score = rev_growth / 0.3 * 100
            print(
                f"[DEBUG] {stock_id} 營收加總：近12月={last_revenue:.0f}，前12月={prev_revenue:.0f} → 營收成長率分數={rev_score:.2f}")

        # 4. 綜合財務面分數
        if eps_df is None and rev_df is None:
            return 0.0
        financial_score = (eps_score + rev_score) / 2
        return financial_score


# --------- 測試範例 (只在直接執行此檔時執行) ----------
if __name__ == "__main__":
    # 1. 取得本檔案所在目錄
    current_dir = os.path.dirname(__file__)
    #   current_dir 例如 ".../stock_analysis/src/analyzers"
    # 2. 往上跳兩層，到專案根目錄，進到 db 資料夾，選 stockDB.db
    db_file = os.path.abspath(os.path.join(
        current_dir, "..", "..", "db", "stockDB.db"))

    print(f"[DEBUG] 連到的資料庫路徑: {db_file}")

    analyzer = FinancialAnalyzer(db_file)

    stock_id_list = ["2330", "2454", "3008", "2610"]
    for sid in stock_id_list:
        print(f"\n=== 開始計算 {sid} ===")
        score = analyzer.get_financial_score(sid)
        print(f"股票 {sid} 的財務面分數：{score:.2f}")
