import sys
import os
import argparse
from datetime import datetime, timedelta
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.utils.helpers import load_config
from src.pipeline.downloader import (
    TWSEDownloader, InstitutionalTWSEDownloader,
    TPExDownloader, TPExInstitutionalDownloader
)
from src.pipeline.twse_to_sqlite import import_twse_csv_to_sqlite
from src.pipeline.twse_institutional_to_sqlite import import_institutional_csv_to_sqlite
from src.pipeline.tpex_to_sqlite import import_tpex_csv_to_sqlite
from src.pipeline.tpex_institutional_to_sqlite import import_tpex_institutional_csv_to_sqlite

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def run_all(date_str, config):
    print(f"\n──── 處理 {date_str} ────")
    raw_dir = config["paths"]["raw_data"]

    # ===== TWSE 股價 =====
    try:
        twse_dir = os.path.join(raw_dir, "twse")
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
            print(f"⚠️ TWSE {date_str} 無交易資料，略過")
    except Exception as e:
        print(f"❌ TWSE {date_str} 失敗：{e}")

    # ===== TWSE 法人 =====
    try:
        inst_dir = os.path.join(raw_dir, "twse_institutional")
        os.makedirs(inst_dir, exist_ok=True)

        inst = InstitutionalTWSEDownloader(
            config["twse_institutional"]["url_template"],
            raw_dir
        )
        csv_path = inst.download(date_str)

        import_institutional_csv_to_sqlite(
            csv_path,
            config["paths"].get("sqlite", "twse.db"),
            config["twse_institutional"].get("table_name", "institutional_chip")
        )
        print(f"✅ TWSE 法人 {date_str} 處理完成")
    except Exception as e:
        print(f"❌ TWSE 法人 {date_str} 失敗：{e}")

    # ===== TPEx 行情 =====
    try:
        tpex_dir = os.path.join(raw_dir, "tpex")
        os.makedirs(tpex_dir, exist_ok=True)

        tp = TPExDownloader(config["tpex"]["url_template"], raw_dir)
        tp.download(date_str)
        csv_path = os.path.join(tpex_dir, f"tpex_{date_str}.csv")

        with open(csv_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
        print(f"[DEBUG] TPEx 行情檔長度: {len(content)}")

        if len(content.strip()) > 100:
            import_tpex_csv_to_sqlite(
                csv_path,
                config["paths"].get("sqlite", "tpex.db"),
                config["tpex"].get("table_name", "tpex_chip"),
                date_str
            )
            print(f"✅ TPEx {date_str} 行情處理完成")
        else:
            print(f"⚠️ TPEx {date_str} 無行情資料，略過")
    except Exception:
        print(f"❌ TPEx {date_str} 行情失敗")
        traceback.print_exc()

    # ===== TPEx 法人 =====
    try:
        inst_dir = os.path.join(raw_dir, "tpex_institutional")
        os.makedirs(inst_dir, exist_ok=True)

        inst = TPExInstitutionalDownloader(inst_dir)
        dt = datetime.strptime(date_str, "%Y%m%d")
        roc_date = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"
        print(f"[INFO] TPEx 法人 ROC 日期: {roc_date}")

        df = inst.download(roc_date)
        csv_name = f"tpex_institutional_{roc_date.replace('/', '')}.csv"
        csv_path = os.path.join(inst_dir, csv_name)

        import_tpex_institutional_csv_to_sqlite(
            csv_path,
            config["paths"].get("sqlite", "tpex.db"),
            config["tpex_institutional"].get("table_name", "tpex_institutional_chip")
        )
        print(f"✅ TPEx 法人 {date_str} 處理完成")
    except Exception:
        print(f"❌ TPEx 法人 {date_str} 失敗")
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="起始日期（YYYYMMDD）", required=True)
    parser.add_argument("--end", help="結束日期（YYYYMMDD），預設今天", default=None)
    args = parser.parse_args()

    config = load_config()
    start = datetime.strptime(args.start, "%Y%m%d")
    end = datetime.strptime(args.end, "%Y%m%d") if args.end else datetime.today()
    print(f"[INFO] start={start.date()}, end={end.date()}")

    for d in daterange(start, end):
        run_all(d.strftime("%Y%m%d"), config)
