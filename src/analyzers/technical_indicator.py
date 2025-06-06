# src/analyzers/technical_indicator.py

import sqlite3
import pandas as pd
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TechnicalIndicatorAnalyzer:
    """
    TechnicalIndicatorAnalyzer 負責：
      1. 保持一個 SQLite 連線（減少反覆開關連線的花費）
      2. 從 twse_price 或 tpex_price 表撈出足夠的近期收盤、最高、最低價
      3. 計算 RSI、MACD_diff、KD_K、KD_D 等指標
      4. 根據最新一筆指標值，計算『技術面分數』（0～100）
    """

    # 表示只取最近這麼多筆資料來計算，若資料不足直接回傳 0 分
    _RECENT_BARS = 60
    _MIN_REQUIRED_BARS = 30

    def __init__(self, db_path: str, market: str = "twse"):
        """
        初始化 TechnicalIndicatorAnalyzer。

        參數：
          - db_path: SQLite 資料庫檔案路徑，例如 "C:/.../stockDB.db"
          - market: "twse" 表示使用 twse_price，"tpex" 表示使用 tpex_price
        """
        if market not in ("twse", "tpex"):
            raise ValueError("market 只能是 'twse' 或 'tpex'")
        self.db_path = db_path
        self.market = market
        # 建立並保留一個連線
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # 如果想觀察 SQL 執行情況可以打開下面這行
        # self.conn.set_trace_callback(logger.debug)

    def __del__(self):
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception:
            pass

    def _fetch_price_data(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        從 twse_price 或 tpex_price 表撈出最近 _RECENT_BARS 筆(日期、收盤、最高、最低)，
        已按照日期正序排序。如果資料少於 _MIN_REQUIRED_BARS，就回傳 None。
        """
        table = "twse_price" if self.market == "twse" else "tpex_price"
        sql = f"""
            SELECT 日期 AS date, 收盤價 AS close, 最高價 AS high, 最低價 AS low
            FROM {table}
            WHERE 證券代號 = ?
            ORDER BY date DESC
            LIMIT {self._RECENT_BARS}
        """
        try:
            df = pd.read_sql_query(sql, self.conn, params=(stock_id,))
        except Exception as e:
            logger.warning(f"[ERROR] 拿 {stock_id} 價格時失敗: {e}")
            return None

        if df.empty or len(df) < self._MIN_REQUIRED_BARS:
            logger.info(
                f"[INFO] {stock_id} 資料筆數 {len(df)} 少於 {self._MIN_REQUIRED_BARS}，跳過計算")
            return None

        # 因為 SQL 是日期 DESC，這裡要轉成正序
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df = df.sort_values("date")
        df = df.set_index("date")
        return df

    @staticmethod
    def _calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
        """
        計算 RSI (Relative Strength Index)。
        """
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _calculate_macd_diff(close: pd.Series,
                             fast_period: int = 12,
                             slow_period: int = 26,
                             signal_period: int = 9) -> pd.Series:
        """
        計算 MACD_diff = MACD 線 (快線 - 慢線) 減掉 訊號線。
        """
        ema_fast = close.ewm(span=fast_period, adjust=False).mean()
        ema_slow = close.ewm(span=slow_period, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        return macd_line - signal_line

    @staticmethod
    def _calculate_kd(close: pd.Series, high: pd.Series, low: pd.Series,
                      period: int = 9, smooth_k: int = 3, smooth_d: int = 3) -> pd.DataFrame:
        """
        計算 KD (Stochastic Oscillator) 指標，輸出 %K 與 %D。
        """
        lowest_low = low.rolling(window=period, min_periods=period).min()
        highest_high = high.rolling(window=period, min_periods=period).max()
        k_raw = 100 * (close - lowest_low) / (highest_high - lowest_low)
        k_smooth = k_raw.rolling(window=smooth_k, min_periods=smooth_k).mean()
        d_smooth = k_smooth.rolling(
            window=smooth_d, min_periods=smooth_d).mean()
        return pd.DataFrame({"K": k_smooth, "D": d_smooth}, index=close.index)

    def calculate_indicators(self, stock_id: str) -> Optional[pd.DataFrame]:
        """
        撈取歷史價格後，計算 RSI、MACD_diff、KD_K (%K)、KD_D (%D)，
        回傳 DataFrame (index=date, columns=["RSI","MACD_diff","KD_K","KD_D"])。
        如果資料不足，回傳 None。
        """
        df_price = self._fetch_price_data(stock_id)
        if df_price is None:
            return None

        close = df_price["close"]
        high = df_price["high"]
        low = df_price["low"]

        # 計算各指標
        rsi = self._calculate_rsi(close, period=14)
        macd_diff = self._calculate_macd_diff(
            close, fast_period=12, slow_period=26, signal_period=9)
        kd = self._calculate_kd(
            close, high, low, period=9, smooth_k=3, smooth_d=3)

        # 合併指標
        df_ind = pd.concat([rsi.rename("RSI"),
                            macd_diff.rename("MACD_diff"),
                            kd.rename(columns={"K": "KD_K", "D": "KD_D"})],
                           axis=1)

        # 去掉有缺值的列，並確認還有至少 1 筆可用來打分
        df_ind = df_ind.dropna()
        if df_ind.empty:
            logger.info(f"[INFO] {stock_id} 所有指標都為 NaN，無法打分")
            return None

        return df_ind

    def get_technical_score(self, stock_id: str) -> float:
        """
        直接從資料庫撈資料、計算指標，並根據最新的指標值計算『技術面分數』 (0~100)。
        沒有足夠資料或出錯則回傳 0.0。
        """
        df_ind = self.calculate_indicators(stock_id)
        if df_ind is None:
            return 0.0

        latest = df_ind.iloc[-1]

        # RSI 打分：RSI ≤30 → 100 分；RSI ≥70 → 0 分；中間線性插值
        rsi_val = latest["RSI"]
        if pd.isna(rsi_val):
            rsi_score = 0.0
        elif rsi_val <= 30:
            rsi_score = 100.0
        elif rsi_val >= 70:
            rsi_score = 0.0
        else:
            rsi_score = (70 - rsi_val) / 40 * 100  # (70-30)=40

        # MACD_diff 打分：假設範圍 -2.0 ~ +2.0；-2→0 分；+2→100 分；中間線性插值
        macd_val = latest["MACD_diff"]
        if pd.isna(macd_val):
            macd_score = 0.0
        else:
            clipped = max(min(macd_val, 2.0), -2.0)
            macd_score = (clipped + 2.0) / 4.0 * 100

        # KD_K 打分：KD_K ≤20 → 100 分；KD_K ≥80 → 0 分；中間線性插值
        kd_k_val = latest["KD_K"]
        if pd.isna(kd_k_val):
            kd_k_score = 0.0
        elif kd_k_val <= 20:
            kd_k_score = 100.0
        elif kd_k_val >= 80:
            kd_k_score = 0.0
        else:
            kd_k_score = (80 - kd_k_val) / 60 * 100  # (80-20)=60

        # KD_D 打分：KD_D ≤20 → 100 分；KD_D ≥80 → 0 分；中間線性插值
        kd_d_val = latest["KD_D"]
        if pd.isna(kd_d_val):
            kd_d_score = 0.0
        elif kd_d_val <= 20:
            kd_d_score = 100.0
        elif kd_d_val >= 80:
            kd_d_score = 0.0
        else:
            kd_d_score = (80 - kd_d_val) / 60 * 100

        # 四個指標分數平均為最終技術面總分
        scores = [rsi_score, macd_score, kd_k_score, kd_d_score]
        tech_score = sum(scores) / len(scores)
        return tech_score


# --------- 測試範例 (只在直接執行此檔時執行) ----------
if __name__ == "__main__":
    import os

    current_dir = os.path.dirname(__file__)
    db_file = os.path.abspath(os.path.join(
        current_dir, "..", "..", "db", "stockDB.db"))
    logger.info(f"連到的資料庫路徑: {db_file}")

    analyzer = TechnicalIndicatorAnalyzer(db_file, market="twse")
    stock_id_list = ["2330", "2454", "2610", "4904", "2303"]
    for sid in stock_id_list:
        score = analyzer.get_technical_score(sid)
        print(f"股票 {sid} 的技術面分數：{score:.2f}")
