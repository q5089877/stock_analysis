#!/usr/bin/env python3
# scripts/download_ticket_all.py

import argparse
from datetime import datetime, timedelta
import sqlite3
import os

from src.utils.config_loader import load_config
from src.pipeline.ticket_downloader import TicketTwseScraper, TicketTpexScraper
from src.pipeline.ticket_sql import import_ticket_twse_sql, import_ticket_tpex_sql


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download ticket HTML (TWSE & TPEX) and import into SQLite."
    )
    parser.add_argument(
        "--start", type=str,
        help="開始日期，格式 YYYYMMDD，預設為今天前 7 天",
        default=None
    )
    parser.add_argument(
        "--end", type=str,
        help="結束日期，格式 YYYYMMDD，預設為今天",
        default=None
    )
    return parser.parse_args()


def daterange(start_date: datetime, end_date: datetime):
    """
    產生從 start_date 到 end_date（含）之間的每日 YYYYMMDD 字串。
    """
    current = start_date
    while current <= end_date:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def has_any_data(sqlite_path: str, table: str, date_str: str) -> bool:
    """
    檢查指定資料表在該 date_str (YYYYMMDD) 是否已有任何一筆資料。
    假設資料表中的日期欄位叫 `date`，且存的值也是 YYYYMMDD 格式。
    只要 table 裡找到一筆 date = date_str，就回傳 True；否則回傳 False。
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
    args = parse_args()
    cfg = load_config()
    raw_dir = cfg["paths"]["raw_data"]
    sqlite_path = cfg["paths"]["sqlite"]

    # 解析日期區間
    today = datetime.today()
    end_date = (
        datetime.strptime(args.end, "%Y%m%d") if args.end else today
    )
    start_date = (
        datetime.strptime(
            args.start, "%Y%m%d") if args.start else today - timedelta(days=7)
    )

    # 建立下載器
    # TWSE 用 YYYYMMDD (不做 YYYY/MM/DD 轉換)
    twse_scraper = TicketTwseScraper(
        cfg["ticket"]["twse_url_template"], raw_dir
    )
    # TPEx 用 YYYY/MM/DD 轉換
    tpex_scraper = TicketTpexScraper(
        cfg["ticket"]["tpex_url_template"], raw_dir
    )

    for date_str in daterange(start_date, end_date):
        print(f"Processing date: {date_str}")

        # ---- TWSE 部分：只檢查 SQL 表是否已有該日期，沒有才下載/匯入 ----
        if has_any_data(sqlite_path, "ticket_twse", date_str):
            print(f"[{date_str}] TWSE ticket SQL 已有任何資料，跳過下載/匯入")
        else:
            try:
                print(f"[{date_str}] 下載 TWSE ticket HTML …")
                twse_path = twse_scraper.fetch(date_str)
                import_ticket_twse_sql(twse_path, sqlite_path)
            except Exception as ex:
                print(f"[{date_str}] ⚠️ TWSE ticket 下載/匯入失敗：{ex}")
            else:
                if has_any_data(sqlite_path, "ticket_twse", date_str):
                    print(f"[{date_str}] ✅ TWSE ticket 匯入完成")
                else:
                    print(f"[{date_str}] ⚠️ TWSE 無任何資料可匯入")

        # ---- TPEx 部分：只檢查 SQL 表是否已有該日期，沒有才下載/匯入 ----
        if has_any_data(sqlite_path, "ticket_tpex", date_str):
            print(f"[{date_str}] TPEX ticket SQL 已有任何資料，跳過下載/匯入")
        else:
            try:
                print(f"[{date_str}] 下載 TPEX ticket HTML …")
                tpex_path = tpex_scraper.fetch(date_str)
                import_ticket_tpex_sql(tpex_path, sqlite_path)
            except Exception as ex:
                print(f"[{date_str}] ⚠️ TPEX ticket 下載/匯入失敗：{ex}")
            else:
                if has_any_data(sqlite_path, "ticket_tpex", date_str):
                    print(f"[{date_str}] ✅ TPEX ticket 匯入完成")
                else:
                    print(f"[{date_str}] ⚠️ TPEX 無任何資料可匯入")

    print("Ticket download and import completed.")


if __name__ == "__main__":
    main()
