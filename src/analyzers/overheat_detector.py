# 檔案：src/analyzers/overheat_detector.py

import os
import pandas as pd
from src.analyzers.technical_indicator import TechnicalIndicatorAnalyzer
from src.utils.config_loader import load_config


class OverheatDetector:
    """
    OverheatDetector 只用 RSI 來判斷是否「過熱」：
      1. 從 config/config.yaml 讀 RSI 閾值 (RSI_threshold)，預設值 80
      2. get_penalty(stock_id):
         - 用 TechnicalIndicatorAnalyzer 拿最新 RSI
         - 若 RSI <= 閾值 → 回傳 0.0
         - 若 RSI >   閾值 → 計算 (RSI - 閾值) / (100 - 閾值)，結果在 0~1 之間，並四捨五入到小數點第二位
    """

    def __init__(self, config_path: str = None):
        # 找到專案根目錄（往上兩層）
        this_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(
            os.path.join(this_dir, os.pardir, os.pardir))

        # 如果沒有傳 config_path，就用專案根目錄下的 config/config.yaml
        if config_path is None:
            config_path = os.path.join(project_root, "config", "config.yaml")

        cfg = load_config(config_path)
        self.rsi_threshold = cfg.get(
            "overheat_detector", {}).get("RSI_threshold", 80)

    def get_penalty(self, stock_id: str) -> float:
        """
        對外方法：給「股票代號」，回傳一個 0~1 的過熱扣分比例：
          - RSI <= 閾值 → 0.0
          - RSI >  閾值 → (RSI - 閾值) / (100 - 閾值)，並裁切到 [0,1]，四捨五入到小數點第 2 位
        """
        ti = TechnicalIndicatorAnalyzer()
        df = ti.calculate(stock_id)
        # 如果沒有 RSI 欄位或表格為空，就回 0
        if df.empty or "RSI" not in df.columns:
            return 0.0

        last_rsi = df["RSI"].dropna().iloc[-1]
        if last_rsi <= self.rsi_threshold:
            return 0.0

        penalty = (last_rsi - self.rsi_threshold) / (100 - self.rsi_threshold)
        if penalty < 0:
            return 0.0
        if penalty > 1:
            return 1.0

        return round(float(penalty), 2)


# 如果要快速測試，可以執行下面程式
if __name__ == "__main__":
    od = OverheatDetector()
    # 假設 2330 的 RSI 目前是 85 → 閾值 80，(85-80)/(100-80)=5/20=0.25
    score = od.get_penalty("2330")
    print(f"2330 的過熱扣分: {score}")
