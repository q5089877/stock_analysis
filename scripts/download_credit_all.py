#!/usr/bin/env python3
# download/download_credit_all.py

import argparse
import os
import time
from datetime import datetime, timedelta
import sqlite3

from requests.exceptions import RequestException
from src.utils.config_loader import load_config
from src.pipeline.twse_credit_sql import import_credit_twse_sql
from src.pipeline.tpex_credit_sql import import_credit_tpex_sql
from src.pipeline.credit_downloader import CreditTwseDownloader, CreditTpexDownloader


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)


def log_error(log_path: str, date: str, market: str, err: Exception):
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(f"{date} {market} 失敗：{err}\n")


def main():
    parser = argparse.ArgumentParser(
        description="下載並匯入 融資／融券 CSV 資料"
    )
    parser.add_argument(
        "--start", help="開始日期 YYYYMMDD (預設 7 天前)", default=None)
    parser.add_argument(
        "--end",   help="結束日期 YYYYMMDD (預設 今天)",    default=None)
    args = parser.parse_args()

    # 讀設定
    cfg = load_config()
    raw_dir = cfg.get("paths", {}).get("raw_data")
    sqlite_path = cfg.get("paths", {}).get("sqlite")
    if not raw_dir or not sqlite_path:
        print("⚠️ 請在 config.yaml 裡設定 paths.raw_data 和 paths.sqlite")
        return

    # 日期參數
    try:
        end_date = datetime.strptime(
            args.end, "%Y%m%d") if args.end else datetime.today()
        start_date = datetime.strptime(
            args.start, "%Y%m%d") if args.start else end_date - timedelta(days=7)
    except ValueError as ve:
        print(f"⚠️ 日期格式錯誤：{ve}，請用 YYYYMMDD")
        return

    # URL 模板
    tw_tpl = cfg.get("credit", {}).get("twse", {}).get("url_template")
    tp_tpl = cfg.get("credit", {}).get("tpex", {}).get("url_template")
    if not tw_tpl or not tp_tpl:
        print("⚠️ 請在 config.yaml 裡設定 credit.twse.url_template 和 credit.tpex.url_template")
        return

    # 初始化下載器
    tw_down = CreditTwseDownloader(tw_tpl, raw_dir)
    tp_down = CreditTpexDownloader(tp_tpl, raw_dir)

    log_path = os.path.join(raw_dir, "download_credit_errors.log")
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # 秒

    print(
        f"下載範圍: {start_date.strftime('%Y%m%d')} ~ {end_date.strftime('%Y%m%d')}")
    for dt in daterange(start_date, end_date):
        # 跳過週六、週日
        dt_obj = datetime.strptime(dt, "%Y%m%d")
        if dt_obj.weekday() >= 5:
            print(f"[{dt}] 週末跳過")
            continue

        # 下載 & 匯入 TWSE
        print(f"[{dt}] 下載 TWSE 融資/融券...")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                csv_tw = tw_down.download(dt)
                print(f"[{dt}] 匯入 credit_twse...")
                import_credit_twse_sql(csv_tw, sqlite_path)
                break  # 成功就跳出重試
            except RequestException as re:
                print(f"  第 {attempt} 次：網路錯誤，{RETRY_DELAY} 秒後再試")
                time.sleep(RETRY_DELAY)
            except FileNotFoundError as fe:
                print(f"  找不到檔案：{fe}")
                log_error(log_path, dt, "TWSE", fe)
                break
            except ValueError as ve:
                print(f"  值錯誤：{ve}")
                log_error(log_path, dt, "TWSE", ve)
                break
            except Exception as e:
                print(f"  其他錯誤：{e}")
                log_error(log_path, dt, "TWSE", e)
                break
        else:
            # 如果 3 次都失敗，記錄總失敗
            log_error(log_path, dt, "TWSE", Exception("重試 3 次仍失敗"))

        # 下載 & 匯入 TPEx
        print(f"[{dt}] 下載 TPEx 融資/融券...")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                csv_tp = tp_down.download(dt)
                print(f"[{dt}] 匯入 credit_tpex...")
                import_credit_tpex_sql(csv_tp, sqlite_path)
                break
            except RequestException as re:
                print(f"  第 {attempt} 次：網路錯誤，{RETRY_DELAY} 秒後再試")
                time.sleep(RETRY_DELAY)
            except FileNotFoundError as fe:
                print(f"  找不到檔案：{fe}")
                log_error(log_path, dt, "TPEx", fe)
                break
            except ValueError as ve:
                print(f"  值錯誤：{ve}")
                log_error(log_path, dt, "TPEx", ve)
                break
            except Exception as e:
                print(f"  其他錯誤：{e}")
                log_error(log_path, dt, "TPEx", e)
                break
        else:
            log_error(log_path, dt, "TPEx", Exception("重試 3 次仍失敗"))

    print("融資/融券資料處理完成！")


if __name__ == "__main__":
    main()
