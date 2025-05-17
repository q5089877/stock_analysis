import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.utils.helpers import load_config
from src.pipeline.downloader import TWSEDownloader, InstitutionalTWSEDownloader
from src.pipeline.twse_to_sqlite import import_twse_csv_to_sqlite
from src.pipeline.twse_institutional_to_sqlite import import_institutional_csv_to_sqlite

def daterange(start_date, end_date):
    """生成從 start_date 到 end_date（含）的所有日期"""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def run_all(date_str, config):
    # ===== 股價 =====
    try:
        twse_dir = os.path.join(config["paths"]["raw_data"], "twse")
        os.makedirs(twse_dir, exist_ok=True)

        twse = TWSEDownloader(config["twse"]["url_template"], twse_dir)
        content = twse.download(date_str)

        if "無任何交易資料" not in content and len(content.strip()) > 200:
            csv_path = os.path.join(twse_dir, f"twse_{date_str}.csv")
            import_twse_csv_to_sqlite(
                csv_path,
                config["paths"].get("sqlite", "twse.db"),
                config["twse"].get("table_name", "twse_chip"),
                date_str
            )
            print(f"✅ TWSE {date_str} 處理完成")
        else:
            print(f"⚠️ TWSE {date_str} 無交易資料，已略過")
    except Exception as e:
        print(f"❌ TWSE {date_str} 處理失敗：{e}")

    # ===== 三大法人 =====
    try:
        inst_dir = os.path.join(config["paths"]["raw_data"], "twse_institutional")
        os.makedirs(inst_dir, exist_ok=True)

        inst = InstitutionalTWSEDownloader(
            config["twse_institutional"]["url_template"],
            config["paths"]["raw_data"]
        )
        csv_path = inst.download(date_str)

        import_institutional_csv_to_sqlite(
            csv_path,
            config["paths"].get("sqlite", "twse.db"),
            config["twse_institutional"].get("table_name", "institutional_chip")
        )
        print(f"✅ 法人 {date_str} 處理完成")
    except Exception as e:
        print(f"❌ 法人 {date_str} 處理失敗：{e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="起始日期（YYYYMMDD）", required=True)
    parser.add_argument("--end",   help="結束日期（YYYYMMDD），預設為今天", default=None)
    args = parser.parse_args()

    config = load_config()
    start = datetime.strptime(args.start, "%Y%m%d")
    end   = datetime.strptime(args.end,   "%Y%m%d") if args.end else datetime.today()

    for date in daterange(start, end):
        run_all(date.strftime("%Y%m%d"), config)
