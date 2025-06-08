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
      2. 從 twse_price 或 tpex_price 表撈出指定日期範圍或最近 _RECENT_BARS 筆資料
      3. 計算 RSI、MACD_diff、KD_K、KD_D 等指標
      4. 根據最新一筆指標值，計算『技術面分數』（0～100）
    """

    # 改為取最近 大約 2 年的交易日數 (~504 天)
    _RECENT_BARS = 60
    # 最少需要的資料筆數（30 天）
    _MIN_REQUIRED_BARS = 30

    def __init__(self, db_path: str, market: str = "twse"):
        if market not in ("twse", "tpex"):
            raise ValueError("market 只能是 'twse' 或 'tpex'")
        self.db_path = db_path
        self.market = market
        # 建立並保留一個連線
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)

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

        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df = df.sort_values("date").set_index("date")
        return df

    @staticmethod
    def _calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _calculate_macd_diff(close: pd.Series,
                             fast_period: int = 12,
                             slow_period: int = 26,
                             signal_period: int = 9) -> pd.Series:
        ema_fast = close.ewm(span=fast_period, adjust=False).mean()
        ema_slow = close.ewm(span=slow_period, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        return macd_line - signal_line

    @staticmethod
    def _calculate_kd(close: pd.Series, high: pd.Series, low: pd.Series,
                      period: int = 9, smooth_k: int = 3, smooth_d: int = 3) -> pd.DataFrame:
        lowest_low = low.rolling(window=period, min_periods=period).min()
        highest_high = high.rolling(window=period, min_periods=period).max()
        k_raw = 100 * (close - lowest_low) / (highest_high - lowest_low)
        k_smooth = k_raw.rolling(window=smooth_k, min_periods=smooth_k).mean()
        d_smooth = k_smooth.rolling(
            window=smooth_d, min_periods=smooth_d).mean()
        return pd.DataFrame({"K": k_smooth, "D": d_smooth}, index=close.index)

    def calculate_indicators(self, stock_id: str) -> Optional[pd.DataFrame]:
        df_price = self._fetch_price_data(stock_id)
        if df_price is None:
            return None

        rsi = self._calculate_rsi(df_price["close"], period=14)
        macd_diff = self._calculate_macd_diff(
            df_price["close"], fast_period=12, slow_period=26, signal_period=9)
        kd = self._calculate_kd(
            df_price["close"], df_price["high"], df_price["low"], period=9, smooth_k=3, smooth_d=3)

        df_ind = pd.concat([
            rsi.rename("RSI"),
            macd_diff.rename("MACD_diff"),
            kd.rename(columns={"K": "KD_K", "D": "KD_D"})
        ], axis=1).dropna()

        if df_ind.empty:
            logger.info(f"[INFO] {stock_id} 所有指標都為 NaN，無法打分")
            return None
        return df_ind

    def get_technical_score(self, stock_id: str) -> float:
        df_ind = self.calculate_indicators(stock_id)
        if df_ind is None:
            return 0.0

        latest = df_ind.iloc[-1]

        def map_score(val, low, high):
            if pd.isna(val):
                return 0.0
            if val <= low:
                return 100.0
            if val >= high:
                return 0.0
            return (high - val) / (high - low) * 100

        scores = [
            map_score(latest["RSI"], 30, 70),
            (max(min(latest["MACD_diff"], 2.0), -2.0) + 2.0) / 4.0 * 100,
            map_score(latest["KD_K"], 20, 80),
            map_score(latest["KD_D"], 20, 80)
        ]
        return sum(scores) / len(scores)


if __name__ == "__main__":
    import os
    current_dir = os.path.dirname(__file__)
    db_file = os.path.abspath(os.path.join(
        current_dir, "..", "..", "db", "stockDB.db"))
    logger.info(f"連到的資料庫路徑: {db_file}")
    analyzer = TechnicalIndicatorAnalyzer(db_file, market="twse")
    sample_ids = ["2330", "2454", "2610", "4904", "2303"]
    for sid in sample_ids:
        print(f"股票 {sid} 的技術面分數：{analyzer.get_technical_score(sid):.2f}")
