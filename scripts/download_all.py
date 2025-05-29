from datetime import datetime, timedelta
import traceback
import argparse
import os
import sys
import sqlite3

from src.pipeline.tpex_inst_sql import import_tpex_inst_sql
from src.pipeline.tpex_price_sql import import_tpex_price_sql
from src.pipeline.twse_inst_sql import import_inst_sql
from src.pipeline.twse_price_sql import import_twse_price_sql
from src.pipeline.downloader import (
    TWSEDownloader, InstitutionalTWSEDownloader,
    TPExDownloader, TPExInstitutionalDownloader,
    TWSEPEDownloader
)
from src.utils.config_loader import load_config

# ── 將專案根目錄加入到模組搜尋路徑 (確保 src/ package 可被找到) ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def check_date_exists(sqlite_path, table_name, date_str):
    """
    檢查資料庫中是否已有指定日期的資料
    """
    if not os.path.exists(sqlite_path):
        return False
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT 1 FROM {table_name} WHERE 日期 = ? LIMIT 1", (date_str,))
        return cur.fetchone() is not None


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def run_all(date_str, config):
    print(f"\n──── 處理 {date_str} ────")
    raw_dir = config["paths"]["raw_data"]

    # ===== TWSE 股價 =====
    try:
        sqlite_path = config["paths"].get("sqlite", "twse.db")
        price_table = config["twse"].get("table_name", "twse_chip")
        if check_date_exists(sqlite_path, price_table, date_str):
            print(f"✅ TWSE 股價 {date_str} 已存在，跳過")
        else:
            twse_dir = os.path.join(raw_dir, "twse")
            os.makedirs(twse_dir, exist_ok=True)

            twse = TWSEDownloader(config["twse"]["url_template"], twse_dir)
            content = twse.download(date_str)

            if "無任何交易資料" not in content and len(content.strip()) > 200:
                csv_path = os.path.join(twse_dir, f"twse_{date_str}.csv")
                import_twse_price_sql(
                    csv_path,
                    sqlite_path,
                    price_table,
                    date_str
                )
                print(f"✅ TWSE {date_str} 股價處理完成")
            else:
                print(f"⚠️ TWSE {date_str} 無交易資料，略過")
    except Exception as e:
        print(f"❌ TWSE 股價 {date_str} 失敗：{e}")

    # ===== TWSE 法人 =====
    try:
        sqlite_path = config["paths"].get("sqlite", "twse.db")
        inst_table = config["twse_institutional"].get(
            "table_name", "twse_institutional_chip")
        if check_date_exists(sqlite_path, inst_table, date_str):
            print(f"✅ TWSE 法人 {date_str} 已存在，跳過")
        else:
            inst_dir = os.path.join(raw_dir, "twse_institutional")
            os.makedirs(inst_dir, exist_ok=True)

            inst = InstitutionalTWSEDownloader(
                config["twse_institutional"]["url_template"],
                raw_dir
            )
            csv_path = inst.download(date_str)

            import_inst_sql(
                csv_path,
                sqlite_path,
                inst_table
            )
            print(f"✅ TWSE 法人 {date_str} 處理完成")
    except Exception as e:
        print(f"❌ TWSE 法人 {date_str} 失敗：{e}")

    # ===== TPEx 行情 =====
    try:
        sqlite_path = config["paths"].get("sqlite", "tpex.db")
        price_table = config["tpex"].get("table_name", "tpex_chip")
        if check_date_exists(sqlite_path, price_table, date_str):
            print(f"✅ TPEx 股價 {date_str} 已存在，跳過")
        else:
            tpex_dir = os.path.join(raw_dir, "tpex")
            os.makedirs(tpex_dir, exist_ok=True)

            tp = TPExDownloader(config["tpex"]["url_template"], raw_dir)
            tp.download(date_str)
            csv_path = os.path.join(tpex_dir, f"tpex_{date_str}.csv")

            with open(csv_path, "r", encoding="utf-8-sig") as f:
                content = f.read()

            if len(content.strip()) > 100:
                import_tpex_price_sql(
                    csv_path,
                    sqlite_path,
                    price_table,
                    date_str
                )
                print(f"✅ TPEx {date_str} 行情處理完成")
            else:
                print(f"⚠️ TPEx {date_str} 無行情資料，略過")
    except Exception:
        print(f"❌ TPEx 股價 {date_str} 行情失敗")
        traceback.print_exc()

    # ===== TPEx 法人 =====
    try:
        sqlite_path = config["paths"].get("sqlite", "tpex.db")
        inst_table = config["tpex_institutional"].get(
            "table_name", "tpex_institutional_chip")
        if check_date_exists(sqlite_path, inst_table, date_str):
            print(f"✅ TPEx 法人 {date_str} 已存在，跳過")
        else:
            inst_dir = os.path.join(raw_dir, "tpex_institutional")
            os.makedirs(inst_dir, exist_ok=True)

            inst = TPExInstitutionalDownloader(inst_dir)
            dt = datetime.strptime(date_str, "%Y%m%d")
            roc_date = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"
            df = inst.download(roc_date)
            csv_name = f"tpex_institutional_{roc_date.replace('/', '')}.csv"
            csv_path = os.path.join(inst_dir, csv_name)

            import_tpex_inst_sql(
                csv_path,
                sqlite_path,
                inst_table
            )
            print(f"✅ TPEx 法人 {date_str} 處理完成")
    except Exception:
        print(f"❌ TPEx 法人 {date_str} 失敗")
        traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    default_start = (datetime.today() - timedelta(days=3)).strftime("%Y%m%d")
    parser.add_argument(
        "--start", help="起始日期（YYYYMMDD），預設為今天前3天", default=default_start
    )
    parser.add_argument(
        "--end", help="結束日期（YYYYMMDD），預設今天", default=None
    )
    args = parser.parse_args()

    config = load_config()
    start = datetime.strptime(args.start, "%Y%m%d")
    end = datetime.strptime(
        args.end, "%Y%m%d") if args.end else datetime.today()
    print(f"[INFO] start={start.date()}, end={end.date()}")

    for d in daterange(start, end):
        run_all(d.strftime("%Y%m%d"), config)
