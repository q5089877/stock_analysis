import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.pipeline.downloader import InstitutionalTWSEDownloader
from src.utils.helpers import load_config
from src.pipeline.twse_institutional_to_sqlite import import_institutional_csv_to_sqlite

def daterange(start_date, end_date):
    """產生日期區間（含首尾）"""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def run_twse_institutional_pipeline(date_str: str, config):
    raw_dir = os.path.join(config["paths"]["raw_data"], "twse_institutional")
    os.makedirs(raw_dir, exist_ok=True)

    # 建立 downloader 並下載
    downloader = InstitutionalTWSEDownloader(
        url_template=config["twse_institutional"]["url_template"],
        save_root=config["paths"]["raw_data"]
    )
    csv_path = downloader.download(date_str)

    # 匯入 SQLite
    sqlite_path = config["paths"].get("sqlite", "twse.db")
    table_name = config.get("twse_institutional", {}).get("table_name", "institutional_chip")
    import_institutional_csv_to_sqlite(csv_path, sqlite_path, table_name)

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
            run_twse_institutional_pipeline(date_str, config)
        except Exception as e:
            print(f"❌ {date_str} 處理失敗：{e}")
