# download/download_ticket_all.py

import argparse
from datetime import datetime, timedelta
from src.utils.config_loader import load_config
from src.pipeline.ticket_downloader import TicketTwseScraper, TicketTpexScraper
from src.pipeline.ticket_sql import import_ticket_twse_sql, import_ticket_tpex_sql


def daterange(start_date: datetime, end_date: datetime):
    """產生從 start_date 到 end_date（包含）的每一天，格式 YYYYMMDD"""
    cur = start_date
    while cur <= end_date:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)


def main():
    # 解析命令列參數
    parser = argparse.ArgumentParser(description="下載並匯入 Ticket HTML 資料")
    parser.add_argument(
        "--start", help="開始日期，格式 YYYYMMDD（預設：今天往前推 7 天）", default=None
    )
    parser.add_argument(
        "--end", help="結束日期，格式 YYYYMMDD（預設：今天）", default=None
    )
    args = parser.parse_args()

    # 讀設定檔
    cfg = load_config()
    raw_dir = cfg["paths"]["raw_data"]
    sqlite_path = cfg["paths"]["sqlite"]

    # 計算日期區間
    today = datetime.today()
    if args.end:
        e = datetime.strptime(args.end, "%Y%m%d")
    else:
        e = today
    if args.start:
        s = datetime.strptime(args.start, "%Y%m%d")
    else:
        s = e - timedelta(days=7)

    print(f"下載日期：{s.strftime('%Y%m%d')} ～ {e.strftime('%Y%m%d')}")

    # 建立下載器
    twse_scraper = TicketTwseScraper(
        cfg["ticket"]["twse_url_template"], raw_dir)
    tpex_scraper = TicketTpexScraper(
        cfg["ticket"]["tpex_url_template"], raw_dir)

    # 依序下載並匯入
    for date_str in daterange(s, e):
        print(f"[{date_str}] 下載 TWSE ticket...")
        twse_path = twse_scraper.fetch(date_str)
        print(f"[{date_str}] 匯入 ticket_twse 資料表...")
        import_ticket_twse_sql(twse_path, sqlite_path)

        print(f"[{date_str}] 下載 TPEX ticket...")
        tpex_path = tpex_scraper.fetch(date_str)
        print(f"[{date_str}] 匯入 ticket_tpex 資料表...")
        import_ticket_tpex_sql(tpex_path, sqlite_path)

    print("全部完成！")


if __name__ == "__main__":
    main()
