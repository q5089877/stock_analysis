#!/usr/bin/env python3
# scripts/download_credit_all.py

import argparse
from datetime import datetime, timedelta
import sqlite3
import os
import pandas as pd

from src.utils.config_loader import load_config
from src.pipeline.credit_downloader import CreditTwseDownloader, CreditTpexDownloader
from src.pipeline.twse_credit_sql import import_credit_twse_sql
from src.pipeline.tpex_credit_sql import import_credit_tpex_sql


def daterange(start: datetime, end: datetime):
    """
    產生從 start 到 end（含）之間的每日 YYYYMMDD 字串。
    """
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def load_valid_ids():
    """
    讀取固定路徑 data/stock_id/stock_id.csv 中的所有有效證券代號
    （去掉非數字、去重複）。
    """
    stock_id_csv = os.path.join("data", "stock_id", "stock_id.csv")
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv：{stock_id_csv}")

    df_ids = pd.read_csv(stock_id_csv, dtype=str, encoding="utf-8-sig")
    valid_ids = (
        df_ids["stock_id"]
        .str.extract(r"(\d+)", expand=False)  # 只抓數字部分
        .fillna("")
        .str.strip()
        .unique()
        .tolist()
    )
    return valid_ids


def has_any_credit_data(sqlite_path: str, table: str, date_str: str) -> bool:
    """
    檢查指定資料表在該日期 (YYYYMMDD) 是否已有任一筆資料。
    只要 table 裡有 date = date_str，就回傳 True；否則回傳 False。
    """
    if not os.path.exists(sqlite_path):
        return False

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT 1 FROM {table} WHERE date = ? LIMIT 1", (date_str,))
        found = cur.fetchone() is not None
    except sqlite3.OperationalError:
        found = False
    conn.close()
    return found


def main():
    parser = argparse.ArgumentParser(
        description="下載並匯入 融資／融券 CSV 資料（含補齊缺少股票）"
    )
    parser.add_argument(
        "--start",
        help="開始日期 YYYYMMDD (預設: 近 7 天前)",
        default=None,
    )
    parser.add_argument(
        "--end",
        help="結束日期 YYYYMMDD (預設: 今天)",
        default=None,
    )
    args = parser.parse_args()

    # 1. 讀取設定檔
    cfg = load_config()
    raw_dir = cfg.get("paths", {}).get("raw_data")
    sqlite_path = cfg.get("paths", {}).get("sqlite")
    if not raw_dir or not sqlite_path:
        print("❌ 配置檔缺少 paths.raw_data 或 paths.sqlite，請檢查 config.yaml")
        return

    # 2. 計算日期區間
    try:
        end_date = (
            datetime.strptime(
                args.end, "%Y%m%d") if args.end else datetime.today()
        )
        start_date = (
            datetime.strptime(args.start, "%Y%m%d")
            if args.start
            else end_date - timedelta(days=7)
        )
    except ValueError as ve:
        print(f"❌ 日期格式錯誤：{ve}，請使用 YYYYMMDD")
        return

    # 3. 取得 URL 模板
    twse_tpl = cfg.get("credit", {}).get("twse", {}).get("url_template")
    tpex_tpl = cfg.get("credit", {}).get("tpex", {}).get("url_template")
    if not twse_tpl or not tpex_tpl:
        print("❌ 配置檔缺少 credit.twse.url_template 或 credit.tpex.url_template，請檢查 config.yaml")
        return

    # 4. 初始化下載器
    tw_down = CreditTwseDownloader(twse_tpl, raw_dir)
    tp_down = CreditTpexDownloader(tpex_tpl, raw_dir)

    # 5. 讀取 stock_id.csv，取得完整的 valid_ids 與預期筆數
    valid_ids = load_valid_ids()
    expected_count = len(valid_ids)

    print(
        f">>> 下載 & 匯入範圍：{start_date.strftime('%Y%m%d')} ～ {end_date.strftime('%Y%m%d')} <<<")
    for dt in daterange(start_date, end_date):
        print(f"\n[{dt}] === 處理 TWSE 融資 ===")
        # 只要表裡有任何該日期資料，就跳過
        if has_any_credit_data(sqlite_path, "credit_twse", dt):
            print(f"[{dt}] TWSE 已有資料，跳過下載/匯入")
        else:
            try:
                print(f"[{dt}] 下載 TWSE CSV …")
                csv_tw = tw_down.download(dt)
            except Exception as ex:
                print(f"[{dt}] ⚠️ 下載 TWSE CSV 失敗：{ex}，跳過匯入。")
                csv_tw = None

            if csv_tw:
                try:
                    import_credit_twse_sql(csv_tw, sqlite_path)
                except Exception as ex:
                    print(f"[{dt}] ⚠️ TWSE 匯入失敗：{ex}")

        print(f"[{dt}] === 處理 TPEx 融資 ===")
        if has_any_credit_data(sqlite_path, "credit_tpex", dt):
            print(f"[{dt}] TPEx 已有資料，跳過下載/匯入")
        else:
            try:
                print(f"[{dt}] 下載 TPEx CSV …")
                csv_tp = tp_down.download(dt)
            except Exception as ex:
                print(f"[{dt}] ⚠️ 下載 TPEx CSV 失敗：{ex}，跳過匯入。")
                csv_tp = None

            if csv_tp:
                try:
                    import_credit_tpex_sql(csv_tp, sqlite_path)
                except Exception as ex:
                    print(f"[{dt}] ⚠️ TPEx 匯入失敗：{ex}")

    print("\n>>> 融資／融券資料處理完成！<<<")


if __name__ == "__main__":
    main()
