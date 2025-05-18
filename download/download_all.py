from datetime import datetime, timedelta
import traceback
import argparse
from src.pipeline.tpex_inst_sql import import_tpex_inst_sql
from src.pipeline.tpex_price_sqlite import import_tpex_price_sql
from src.pipeline.twse_yield_sql import import_twse_yield_sql
from src.pipeline.twse_inst_sql import import_inst_sql
from src.pipeline.twse_price_sql import import_twse_price_sql
from src.pipeline.downloader import (
    TWSEDownloader, InstitutionalTWSEDownloader,
    TPExDownloader, TPExInstitutionalDownloader,
    TWSEPEDownloader
)
from src.utils.helpers import load_config
import os
import sys

# ── 將專案根目錄加入到模組搜尋路徑 (確保 src/ package 可被找到) ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ===== 現在再進行 src.pipeline 和 src.utils 的匯入 =====


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
            import_twse_price_sql(
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

        import_inst_sql(
            csv_path,
            config["paths"].get("sqlite", "twse.db"),
            config["twse_institutional"].get(
                "table_name", "twse_institutional_chip")
        )
        print(f"✅ TWSE 法人 {date_str} 處理完成")
    except Exception as e:
        print(f"❌ TWSE 法人 {date_str} 失敗：{e}")

    # ===== TWSE 殖利率 & 股價淨值比 =====
    try:
        pe_dir = os.path.join(raw_dir, "twse_pe")
        os.makedirs(pe_dir, exist_ok=True)

        # 1) 下載 CSV
        pe_down = TWSEPEDownloader(pe_dir)
        csv_path = pe_down.download(date_str)

        import_twse_yield_sql(
            csv_path,
            config["paths"].get("sqlite", "twse.db"),
            config.get("twse_yield_pb", {}).get("table_name", "twse_yield_pb")
        )
        print(f"✅ TWSE 殖利率／PB {date_str} 建表並匯入完成")
    except Exception as e:
        print(f"❌ TWSE 殖利率／PB {date_str} 失敗：{e}")

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
            import_tpex_price_sql(
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

        # ===== TPEx 殖利率 & 股價淨值比 & 本益比 =====
    try:
        tpex_pe_dir = os.path.join(raw_dir, "tpex_pe")
        os.makedirs(tpex_pe_dir, exist_ok=True)

        # 1) 下載 CSV
        from src.pipeline.downloader import TPEXPEDownloader  # 確保這已存在於 downloader.py
        pe_down = TPEXPEDownloader(tpex_pe_dir)
        csv_path = pe_down.download(date_str)

        from src.pipeline.tpex_yield_sqlite import import_tpex_yield_sql
        import_tpex_yield_sql(
            csv_path,
            config["paths"].get("sqlite", "tpex.db"),
            config.get("tpex_yield_pb", {}).get("table_name", "tpex_yield_pb")
        )
        print(f"✅ TPEx 殖利率／PB／PE {date_str} 建表並匯入完成")
    except Exception as e:
        print(f"❌ TPEx 殖利率／PB／PE {date_str} 失敗：{e}")

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

        import_tpex_inst_sql(
            csv_path,
            config["paths"].get("sqlite", "tpex.db"),
            config["tpex_institutional"].get(
                "table_name", "tpex_institutional_chip")
        )
        print(f"✅ TPEx 法人 {date_str} 處理完成")
    except Exception:
        print(f"❌ TPEx 法人 {date_str} 失敗")
        traceback.print_exc()

    # ===== 財報基本面指標（ROE & 毛利率） =====
    try:
        from src.pipeline.finance_to_sqlite import import_finance_indicators
        import_finance_indicators(config)
        print(f"✅ 財務指標處理完成")
    except Exception as e:
        print(f"❌ 財務指標處理失敗：{e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="起始日期（YYYYMMDD）", required=True)
    parser.add_argument("--end", help="結束日期（YYYYMMDD），預設今天", default=None)
    args = parser.parse_args()

    config = load_config()
    start = datetime.strptime(args.start, "%Y%m%d")
    end = datetime.strptime(
        args.end, "%Y%m%d") if args.end else datetime.today()
    print(f"[INFO] start={start.date()}, end={end.date()}")

    for d in daterange(start, end):
        run_all(d.strftime("%Y%m%d"), config)
