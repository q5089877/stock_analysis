import sqlite3
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

###############################################################################
# Borrow Analyzer – v2                                                         #
# --------------------------------------------------------------------------- #
#  • Schema validation (table + columns)                                       #
#  • Adjustable look‑back window (days)                                        #
#  • Missing‑value aware (keeps NaN, no blind fill(0))                         #
#  • Small‑limit normalisation: util * sqrt(limit / max_limit)                 #
#  • Flexible trend methods: "slope" | "delta" | "ewma"                    #
#  • Customisable weights + thresholds                                         #
###############################################################################

REQUIRED_COLUMNS = {
    "date",
    "股票代號",
    "借券當日餘額",
    "借券限額",
}
TABLE_NAME = "ticket_twse"


def clamp(x: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, x))


class BorrowAnalyzer:
    """計算借券安全分數 (0–100)，高分代表空方壓力低。"""

    # ---------------------------------------------------------------------
    # Construction
    # ---------------------------------------------------------------------
    def __init__(
        self,
        db_path: str = "db/stockDB.db",
        *,
        days: int = 20,
        micro_util: float = 1e-3,
        micro_slope: float = 5e-4,
        weight_today: float = 0.5,
        weight_trend: float = 0.5,
        trend_method: str = "slope",  # "slope" | "delta" | "ewma"
        util_scale_by_limit: bool = True,
    ) -> None:
        self.db_path = db_path
        self.days = days
        self.micro_util = micro_util
        self.micro_slope = micro_slope
        self.weight_today = weight_today
        self.weight_trend = weight_trend
        self.trend_method = trend_method
        self.util_scale_by_limit = util_scale_by_limit

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def score(self, stock_ids: Sequence[str]) -> Dict[str, Optional[float]]:
        """回傳 {stock_id: score or None}."""
        stock_ids = list(dict.fromkeys(stock_ids))  # 去重、保持順序
        result: Dict[str, Optional[float]] = {sid: None for sid in stock_ids}

        # 1) 連線
        try:
            conn = sqlite3.connect(self.db_path)
        except Exception as e:
            raise RuntimeError(f"無法開啟資料庫 {self.db_path}: {e}") from e

        # 2) 檢查 schema
        self._validate_schema(conn)

        # 3) 最近 N 天日期
        last_days = self._latest_dates(conn, self.days)
        if len(last_days) < self.days:
            # 資料不足
            conn.close()
            return result

        # 4) 撈資料
        df = self._load_data(conn, last_days)
        conn.close()
        if df.empty:
            return result

        # 5) util 計算
        df["實際限額"] = df["借券限額"].astype(float) * 1000.0
        df["util"] = np.where(
            df["借券限額"].astype(float) == 0.0,
            np.nan,
            df["借券當日餘額"].astype(float) / df["實際限額"],
        )

        # 小限額修正 → 把 util 乘上 sqrt(limit / max_limit)，避免極小限額被放大
        if self.util_scale_by_limit:
            max_limit = df["實際限額"].max()
            limit_weight = np.sqrt(df["實際限額"] / max_limit)
            df["util_scaled"] = df["util"] * limit_weight
        else:
            df["util_scaled"] = df["util"]

        # 換掉微小值 (< micro_util) → 0
        df["util_scaled"] = df["util_scaled"].where(
            df["util_scaled"].abs() >= self.micro_util, 0.0
        )

        # pivot
        util_pvt = (
            df.pivot(index="股票代號", columns="date", values="util_scaled")
            .reindex(stock_ids)
        )

        valid_mask = util_pvt.notna().any(axis=1)
        valid_ids = util_pvt.index[valid_mask].tolist()
        if not valid_ids:
            return result

        # 6) 今日分數
        today_col = last_days[0]
        today_series = util_pvt.loc[valid_ids, today_col].fillna(0.0)
        rank_today = today_series.rank(pct=True, method="average")
        score_today = (1.0 - rank_today) * 100.0

        # 7) 趨勢分數
        trend_series = self._calc_trend(
            util_pvt.loc[valid_ids, last_days], last_days)
        rank_trend = trend_series.rank(pct=True, method="average")
        score_trend = (1.0 - rank_trend) * 100.0

        # 8) 彙總
        for sid in valid_ids:
            st = clamp(score_today[sid])
            tt = clamp(score_trend[sid])
            final = (
                self.weight_today * st + self.weight_trend * tt
            ) / (self.weight_today + self.weight_trend)
            result[sid] = round(clamp(final), 2)

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _validate_schema(self, conn: sqlite3.Connection) -> None:
        qry = f"PRAGMA table_info({TABLE_NAME})"
        cols = {row[1] for row in conn.execute(qry)}
        missing = REQUIRED_COLUMNS - cols
        if missing:
            raise RuntimeError(
                f"{TABLE_NAME} 缺少欄位: {', '.join(missing)}"
            )

    def _latest_dates(self, conn: sqlite3.Connection, n: int) -> List[str]:
        qry = (
            f"SELECT DISTINCT date FROM {TABLE_NAME} ORDER BY date DESC LIMIT ?"
        )
        return [row[0] for row in conn.execute(qry, (n,))]

    def _load_data(self, conn: sqlite3.Connection, dates: Sequence[str]) -> pd.DataFrame:
        placeholder = ",".join(["?"] * len(dates))
        qry = (
            f"SELECT date, 股票代號, 借券當日餘額, 借券限額 "
            f"FROM {TABLE_NAME} WHERE date IN ({placeholder})"
        )
        return pd.read_sql_query(qry, conn, params=list(dates))

    def _calc_trend(
        self, util_df: pd.DataFrame, dates: Sequence[str]
    ) -> pd.Series:
        method = self.trend_method
        x = np.arange(len(dates))

        if method == "delta":
            # 最後 − 最前，除以天數正規化
            delta = util_df[dates[-1]] - util_df[dates[0]]
            return delta / len(dates)

        if method == "ewma":
            # EWMA slope proxy: diff of EWMA vs simple mean
            ewma = util_df.apply(
                lambda row: row[::-1].ewm(span=len(dates)//2).mean().iloc[0],
                axis=1,
            )
            mean_last = util_df.mean(axis=1)
            return ewma - mean_last

        # default: slope of linear fit
        slopes = []
        for _, row in util_df.iterrows():
            y = row.values.astype(float)
            # if all nan
            if np.isnan(y).all():
                slopes.append(np.nan)
                continue
            # nan → linearly interpolate
            y = pd.Series(y).interpolate(limit_direction="both").values
            m = np.polyfit(x, y, 1)[0]
            if abs(m) < self.micro_slope:
                m = 0.0
            slopes.append(m)
        return pd.Series(slopes, index=util_df.index, dtype=float)


if __name__ == "__main__":
    # 測試 main
    import os

    # 讀取 stock_id.csv 裡的所有股票 ID
    csv_path = os.path.join("data", "stock_id", "stock_id.csv")
    try:
        df_ids = pd.read_csv(csv_path, dtype=str)
        all_stocks = df_ids.iloc[:, 0].str.strip().tolist()
    except Exception as e:
        print(f"讀取 {csv_path} 失敗：{e}")
        all_stocks = []

    if not all_stocks:
        print(f"{csv_path} 裡沒有任何股票代號，或是讀取失敗。")
    else:
        analyzer = BorrowAnalyzer(
            db_path="db/stockDB.db",
            days=20,
            trend_method="slope",
            weight_today=0.5,
            weight_trend=0.5,
            util_scale_by_limit=True,
        )
        scores = analyzer.score(all_stocks)

        print("借券分析分數（v2，最近 20 天）：")
        for sid in all_stocks:
            s = scores.get(sid)
            if s is None:
                pass
                # print(f"{sid}：無法計算分數。")
            else:
                print(f"{sid}：{s:.2f} 分")
