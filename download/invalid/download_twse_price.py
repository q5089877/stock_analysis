from src.pipeline.twse_price_sql import import_twse_price_sql  # 前面建立的清洗函式
from src.utils.helpers import load_config
from src.pipeline.downloader import TWSEDownloader
import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


def daterange(start_date, end_date):
    """產生日期區間生成器（含 start 與 end）"""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def run_twse_pipeline(date_str: str, config):
    raw_dir = os.path.join(config["paths"]["raw_data"], "twse")
    os.makedirs(raw_dir, exist_ok=True)
    csv_filename = f"twse_{date_str}.csv"
    csv_path = os.path.join(raw_dir, csv_filename)

    # 執行下載
    twse = TWSEDownloader(
        url_template=config["twse"]["url_template"],
        save_dir=raw_dir
    )
    content = twse.download(date_str)

    # 判斷是否有交易資料
    if "無任何交易資料" in content or len(content.strip()) < 200:
        print(f"⚠️ {date_str} 無交易資料，已跳過")
        return

    # 匯入 SQLite
    sqlite_path = config["paths"].get("sqlite", "twse.db")
    table_name = config.get("twse", {}).get("table_name", "twse_chip")
    import_twse_price_sql(csv_path, sqlite_path, table_name, date_str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="起始日期（格式：YYYYMMDD）")
    parser.add_argument("--end", required=True, help="結束日期（格式：YYYYMMDD）")
    args = parser.parse_args()

    config = load_config()
    start = datetime.strptime(args.start, "%Y%m%d")
    end = datetime.strptime(args.end, "%Y%m%d")

    for date in daterange(start, end):
        date_str = date.strftime("%Y%m%d")
        try:
            run_twse_pipeline(date_str, config)
        except Exception as e:
            print(f"❌ {date_str} 處理失敗：{e}")
