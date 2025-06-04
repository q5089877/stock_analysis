# src/analyzers/technical_indicator.py

import os
import sqlite3
import pandas as pd
from typing import Optional


class TechnicalIndicatorAnalyzer:
    """
    TechnicalIndicatorAnalyzer 負責：
      1. 從 SQLite 資料庫撈出台灣（TWSE）或櫃買（TPEX）的歷史價格資料，
      2. 計算各種技術指標 (RSI、MACD_diff、KD_K、KD_D)，
      3. 再依照最新一筆指標值，計算『技術面分數』（0～100）。
    """

    def __init__(self, db_path: str, market: str = "twse"):
        """
        初始化 TechnicalIndicatorAnalyzer。

        參數：
          - db_path: str
              SQLite 資料庫檔案路徑，例如 "C:/.../stock_analysis/db/stockDB.db"。
          - market: str
              市場代碼，"twse" 表示使用 twse_price 資料表，
                         "tpex" 表示使用 tpex_price 資料表。
        """
        self.db_path = db_path
        if market not in ("twse", "tpex"):
            raise ValueError("market 只能是 'twse' 或 'tpex'")
        self.market = market

    def _debug_list_tables(self):
        """
        列出目前連到的 SQLite 資料庫裡，所有的 table 名稱。
        用來檢查是否有 twse_price 這張表。
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

    def _fetch_price_data(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從 twse_price 或 tpex_price 表撈出收盤、最高、最低價 (按日期排序)。
        """
        table_name = "twse_price" if self.market == "twse" else "tpex_price"
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"""
                SELECT 日期 AS date, 收盤價 AS close, 最高價 AS high, 最低價 AS low
                FROM {table_name}
                WHERE 證券代號 = ?
                ORDER BY date ASC
            """
            df = pd.read_sql_query(query, conn, params=(stock_id,))
            conn.close()
            if df.empty:
                return None
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
            df.set_index("date", inplace=True)
            return df
        except Exception as e:
            print(f"[ERROR] 抓價格資料失敗: {e}")
            return None

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        計算 RSI (Relative Strength Index)。
        """
        delta = prices.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_macd_diff(self, prices: pd.Series,
                             fast_period: int = 12,
                             slow_period: int = 26,
                             signal_period: int = 9) -> pd.Series:
        """
        計算 MACD_diff = MACD 線 (快線 - 慢線) 減掉 訊號線。
        """
        ema_fast = prices.ewm(span=fast_period, adjust=False).mean()
        ema_slow = prices.ewm(span=slow_period, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        macd_diff = macd_line - signal_line
        return macd_diff

    def _calculate_kd(self, prices: pd.Series, high: pd.Series, low: pd.Series,
                      period: int = 9, smooth_k: int = 3, smooth_d: int = 3) -> pd.DataFrame:
        """
        計算 KD (Stochastic Oscillator) 指標，輸出 %K 與 %D。
        """
        lowest_low = low.rolling(window=period, min_periods=period).min()
        highest_high = high.rolling(window=period, min_periods=period).max()
        k_raw = 100 * (prices - lowest_low) / (highest_high - lowest_low)
        k_smooth = k_raw.rolling(window=smooth_k, min_periods=smooth_k).mean()
        d_smooth = k_smooth.rolling(
            window=smooth_d, min_periods=smooth_d).mean()
        kd_df = pd.DataFrame({"K": k_smooth, "D": d_smooth})
        return kd_df

    def calculate_indicators(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從資料庫撈出歷史價格，計算 RSI、MACD_diff、KD_K、KD_D 等指標，
        並回傳一張 DataFrame，欄位包含：
          - RSI
          - MACD_diff
          - KD_K (即 %K)
          - KD_D (即 %D)
        如果資料不足，回傳 None。
        """
        df_price = self._fetch_price_data(stock_id)
        if df_price is None:
            print(f"[DEBUG] {stock_id} 無價格資料 (df_price is None)")
            return None

        print(f"[DEBUG] {stock_id} 原始價格資料列數：{len(df_price)}")
        if len(df_price) < 30:
            print(f"[DEBUG] {stock_id} 資料少於 30 天，無法計算指標")
            return None

        df_price["RSI"] = self._calculate_rsi(df_price["close"], period=14)
        df_price["MACD_diff"] = self._calculate_macd_diff(df_price["close"],
                                                          fast_period=12,
                                                          slow_period=26,
                                                          signal_period=9)
        kd_df = self._calculate_kd(df_price["close"], df_price["high"], df_price["low"],
                                   period=9, smooth_k=3, smooth_d=3)
        df_price["KD_K"] = kd_df["K"]
        df_price["KD_D"] = kd_df["D"]

        df_ind = df_price[["RSI", "MACD_diff", "KD_K", "KD_D"]].dropna()
        print(f"[DEBUG] {stock_id} 計算後指標資料列數：{len(df_ind)}")
        return df_ind

    def get_technical_score(self, stock_id: str) -> float:
        """
        直接從資料庫撈資料、計算指標，並根據最新的指標值計算『技術面分數』 (0~100)。
        """
        # 先列出資料庫裡的所有表，確認是否有 twse_price
        self._debug_list_tables()

        df_ind = self.calculate_indicators(stock_id)
        if df_ind is None:
            print(f"[DEBUG] {stock_id} df_ind is None → 回傳 0.0")
            return 0.0
        if df_ind.empty:
            print(f"[DEBUG] {stock_id} df_ind 是空的 → 回傳 0.0")
            return 0.0

        latest = df_ind.iloc[-1]

        # RSI 打分 (RSI ≤30 → 100 分；RSI ≥70 → 0 分；中間線性插值)
        rsi_val = latest["RSI"]
        if pd.isna(rsi_val):
            rsi_score = 0.0
        elif rsi_val <= 30:
            rsi_score = 100.0
        elif rsi_val >= 70:
            rsi_score = 0.0
        else:
            rsi_score = (70 - rsi_val) / (70 - 30) * 100

        # MACD_diff 打分 (假設範圍 -2.0 ~ +2.0；-2→0 分；+2→100 分；中間線性插值)
        macd_val = latest["MACD_diff"]
        if pd.isna(macd_val):
            macd_score = 0.0
        else:
            clipped = max(min(macd_val, 2.0), -2.0)
            macd_score = (clipped + 2.0) / 4.0 * 100

        # KD_K 打分 (KD_K ≤20 → 100 分；KD_K ≥80 → 0 分；中間線性插值)
        kd_k_val = latest["KD_K"]
        if pd.isna(kd_k_val):
            kd_k_score = 0.0
        elif kd_k_val <= 20:
            kd_k_score = 100.0
        elif kd_k_val >= 80:
            kd_k_score = 0.0
        else:
            kd_k_score = (80 - kd_k_val) / (80 - 20) * 100

        # KD_D 打分 (KD_D ≤20 → 100 分；KD_D ≥80 → 0 分；中間線性插值)
        kd_d_val = latest["KD_D"]
        if pd.isna(kd_d_val):
            kd_d_score = 0.0
        elif kd_d_val <= 20:
            kd_d_score = 100.0
        elif kd_d_val >= 80:
            kd_d_score = 0.0
        else:
            kd_d_score = (80 - kd_d_val) / (80 - 20) * 100

        # 四個指標分數平均為最終技術面總分
        scores = [rsi_score, macd_score, kd_k_score, kd_d_score]
        tech_score = sum(scores) / len(scores)
        return tech_score


# --------- 測試範例 (只在直接執行此檔時執行) ----------
if __name__ == "__main__":
    # 1. 取得本檔案所在目錄
    current_dir = os.path.dirname(__file__)
    #    current_dir 例如 ".../stock_analysis/src/analyzers"
    # 2. 往上跳兩層，到專案根目錄，然後再進到 db 資料夾，選 stockDB.db
    db_file = os.path.abspath(os.path.join(
        current_dir, "..", "..", "db", "stockDB.db"))

    print(f"[DEBUG] 連到的資料庫路徑: {db_file}")

    analyzer = TechnicalIndicatorAnalyzer(db_file, market="twse")

    stock_id_list = ["2330", "2454", "2610"]
    for sid in stock_id_list:
        print(f"\n=== 開始計算 {sid} ===")
        score = analyzer.get_technical_score(sid)
        print(f"股票 {sid} 的技術面分數：{score:.2f}")
