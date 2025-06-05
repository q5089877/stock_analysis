# src/utils/stock_list_loader.py

import os
import csv


def load_stock_list(config):
    """
    讀取 stock_id.csv，回傳股票代號的清單（list of str）。

    參數：
      - config (dict)：由 load_config() 回傳的設定 dict，裡面要包含
          paths:
            stock_list: "data/stock_id/stock_id.csv"

    回傳：
      - stocks (list of str)：所有讀到的股票代號，例如 ["2330", "2317", ...]
    """

    # 先從設定裡取出 stock_list 的相對路徑
    stock_list_rel = config.get("paths", {}).get("stock_list")
    if not stock_list_rel:
        raise KeyError("config 裡缺少 paths → stock_list 設定")

    # 計算從專案根目錄開始的完整路徑（跟 config_loader.py 的做法一樣）
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.normpath(
        os.path.join(current_dir, os.pardir, os.pardir))
    stock_list_path = os.path.join(project_root, stock_list_rel)

    if not os.path.exists(stock_list_path):
        raise FileNotFoundError(f"找不到股票代號檔案: {stock_list_path}")

    stocks = []
    # 假設 CSV 檔第一行是標題，後面每行第一欄就是股票代號
    with open(stock_list_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        # 讀到的每一行是 list，例如 ["2330", "台積電"]
        # 我們只要第一欄數值作為股票代號
        header = next(reader, None)  # 跳過標題列
        for row in reader:
            if row and row[0].strip():
                stocks.append(row[0].strip())

    return stocks
