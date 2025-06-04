# 檔案：src/analyzers/performance_tracker.py

import os
import sqlite3
import pandas as pd
from src.utils.config_loader import load_config

class PerformanceTracker:
    """
    這個類別負責「算股票績效」，包含：
      1. 勝率 (win rate)：漲的天數 / 總交易天數
      2. 平均報酬 (average return)：每天的漲跌幅平均
      3. 最大回撤 (max drawdown)：累積報酬最高點到最低點之間的跌幅

    使用前，請先在 config/config.yaml 裡面設定：
      paths:
        sqlite: "db/stockDB.db"           # SQLite 資料庫路徑
      tables:
        price_table: "twse_price"         # 價格資料表名稱
      performance_tracker:
        backtest_start: "2024-01-01"      # 回測開始日期 (YYYY-MM-DD)
        backtest_end: "2024-12-31"        # 回測結束日期 (YYYY-MM-DD)
    """

    def __init__(self, config_path: str = None):
        # 如果沒有傳 config_path，就自動找專案根目錄下的 config/config.yaml
        this_dir = os.path.dirname(__file__)               # e.g. .../src/analyzers
        project_root = os.path.abspath(os.path.join(this_dir, os.pardir, os.pardir))

        if config_path is None:
            config_path = os.path.join(project_root, "config", "config.yaml")

        # 讀設定檔，拿 SQLite 路徑與表格名稱，以及回測日期
        cfg = load_config(config_path)
        self.sqlite_path = cfg.get("paths", {}).get("sqlite")
        if not self.sqlite_path:
            raise KeyError("config.yaml 裡面必須有 paths.sqlite 設定")

        self.price_table = cfg.get("tables", {}).get("price_table", "twse_price")

        perf_cfg = cfg.get("performance_tracker", {})
        # 回測開始／結束日期：如果沒設定，就不套日期限制
        self.start_date = perf_cfg.get("backtest_start", None)
        self.end_date = perf_cfg.get("backtest_end", None)

    def get_price_df(self, stock_id: str) -> pd.DataFrame:
        """
        從 SQLite 抓取指定股票在回測期間內的收盤價 (close price)。
        回傳 DataFrame，index: date (datetime)，欄位: 'close'。
        """
        conn = sqlite3.connect(self.sqlite_path)
        query = f"""
            SELECT 日期 as date, 收盤價 as close
            FROM {self.price_table}
            WHERE 證券代號 = ?
              { "AND 日期 >= ?" if self.start_date else "" }
              { "AND 日期 <= ?" if self.end_date else "" }
            ORDER BY 日期
        """
        params = [stock_id]
        if self.start_date:
            # 把 config 裡的 YYYY-MM-DD 轉成 YYYYMMDD，再傳給 SQLite
            sd = pd.to_datetime(self.start_date).strftime("%Y%m%d")
            params.append(sd)
        if self.end_date:
            ed = pd.to_datetime(self.end_date).strftime("%Y%m%d")
            params.append(ed)

        df = pd.read_sql_query(query, conn, params=params, parse_dates=["date"])
        conn.close()

        # 如果沒資料，回傳空
        if df.empty:
            return pd.DataFrame(columns=["date", "close"]).set_index("date")

        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df = df.set_index("date").sort_index()
        return df[["close"]]

    def compute_metrics(self, stock_id: str) -> dict:
        """
        對外唯一要呼叫的方法：
          1. 讀收盤價
          2. 算「日報酬」 = (今天收盤 - 昨天收盤) / 昨天收盤
          3. 勝率 = 日報酬 > 0 的天數 / (非零日報酬天數)
          4. 平均報酬 = 日報酬的平均
          5. 最大回撤 = (累積報酬走勢最高到最低的跌幅)

        回傳字典：
          {
            "win_rate": float,
            "avg_return": float,
            "max_drawdown": float
          }
        """

        df_price = self.get_price_df(stock_id)
        if df_price.empty or len(df_price) < 2:
            # 如果資料不夠，就全部設為 0
            return {"win_rate": 0.0, "avg_return": 0.0, "max_drawdown": 0.0}

        # 1. 計算每天的報酬率（今天 close / 昨天 close - 1）
        df_price["return"] = df_price["close"].pct_change()

        # 去掉第一筆 NaN
        returns = df_price["return"].dropna()

        # 2. 勝率 = 報酬 > 0 的天數 / 總天數
        num_positive = (returns > 0).sum()
        total_days = len(returns)
        win_rate = num_positive / total_days if total_days > 0 else 0.0

        # 3. 平均報酬
        avg_return = returns.mean()

        # 4. 最大回撤
        #    先算「累積報酬走勢」：cum_return_t = (1 + r1)*(1 + r2)*...*(1 + rt)
        cum = (1 + returns).cumprod()
        running_max = cum.cummax()
        #    回撤序列 = (cum - running_max) / running_max
        drawdown = (cum - running_max) / running_max
        max_drawdown = drawdown.min()  # 最小值（通常是負的），例如 -0.15 代表 15% 回撤

        # 回傳
        return {
            "win_rate": float(win_rate),
            "avg_return": float(avg_return),
            # 取絕對值（讓回撤正值表示跌幅），若想要保留負值可以不 abs
            "max_drawdown": float(abs(max_drawdown))
        }


# 如果想做簡易測試，可在這裡撰寫
if __name__ == "__main__":
    # 測試用例：看股票 2330 在回測期間內的績效
    pt = PerformanceTracker()
    metrics = pt.compute_metrics("2330")
    print(f"股票 2330 的績效：")
    print(f"  勝率 (Win Rate)：{metrics['win_rate']:.2%}")
    print(f"  平均日報酬 (Avg Return)：{metrics['avg_return']:.4f}")
    print(f"  最大回撤 (Max Drawdown)：{metrics['max_drawdown']:.2%}")
