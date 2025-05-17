#!/usr/bin/env python3
from src.pipeline.tpex_inst_sql import import_tpex_inst_sql
from src.pipeline.tpex_price_sqlite import import_tpex_price_sql
from src.pipeline.downloader import TPExDownloader, TPExInstitutionalDownloader
from src.utils.helpers import load_config
import sys
import os
import argparse
from datetime import datetime, timedelta
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


def daterange(start_date, end_date):
    """生成從 start_date 到 end_date（含）的所有日期"""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def run_all(date_str, config):
    print(f"\n──── 開始處理 {date_str} ────")
    raw_dir = config["paths"]["raw_data"]

    # ===== 櫃買中心行情 =====
    try:
        tpex_dir = os.path.join(raw_dir, "tpex")
        os.makedirs(tpex_dir, exist_ok=True)

        # 下載並存檔
        tp = TPExDownloader(config["tpex"]["url_template"], raw_dir)
        tp.download(date_str)
        csv_path = os.path.join(tpex_dir, f"tpex_{date_str}.csv")
        print(f"[INFO] TPEx 行情 CSV: {csv_path}")

        # 重新讀檔判斷是否有資料
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
        print(f"[DEBUG] 行情檔長度: {len(content)}")

        if len(content.strip()) > 100:
            import_tpex_price_sql(
                csv_path,
                config["paths"].get("sqlite", "tpex.db"),
                config["tpex"].get("table_name", "tpex_chip"),
                date_str
            )
            print(f"✅ TPEx {date_str} 行情處理完成")
        else:
            print(f"⚠️ TPEx {date_str} 無行情資料，跳過")
    except Exception:
        print(f"❌ TPEx {date_str} 行情失敗")
        traceback.print_exc()

    # ===== 櫃買三大法人 =====
    try:
        inst_dir = os.path.join(raw_dir, "tpex_institutional")
        os.makedirs(inst_dir, exist_ok=True)

        inst = TPExInstitutionalDownloader(inst_dir)
        # 西元 → 民國
        dt = datetime.strptime(date_str, "%Y%m%d")
        roc_date = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"
        print(f"[INFO] 下載法人資料，ROC 日期: {roc_date}")

        df = inst.download(roc_date)
        print(
            f"[DEBUG] 法人資料 rows: {len(df) if hasattr(df, '__len__') else 'N/A'}")

        # 下載後產出的 CSV 檔
        csv_name_inst = f"tpex_institutional_{roc_date.replace('/', '')}.csv"
        csv_path_inst = os.path.join(inst_dir, csv_name_inst)
        print(f"[INFO] 法人 CSV: {csv_path_inst}")

        import_tpex_inst_sql(
            csv_path_inst,
            config["paths"].get("sqlite", "tpex.db"),
            config["tpex_institutional"].get(
                "table_name", "tpex_institutional_chip")
        )
        print(f"✅ TPEx {date_str} 法人處理完成")
    except Exception:
        print(f"❌ TPEx {date_str} 法人失敗")
        traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="起始日期（YYYYMMDD）", required=True)
    parser.add_argument("--end",   help="結束日期（YYYYMMDD），預設今天", default=None)
    args = parser.parse_args()

    config = load_config()
    start = datetime.strptime(args.start, "%Y%m%d")
    end = datetime.strptime(
        args.end,   "%Y%m%d") if args.end else datetime.today()
    print(f"[INFO] start={start.date()}, end={end.date()}")

    for d in daterange(start, end):
        run_all(d.strftime("%Y%m%d"), config)
