import os
import re
import sqlite3
import pandas as pd
from datetime import datetime


def import_tpex_price_sql(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "tpex_chip",
    date_str: str = ''
):
    """
    將 TPEx 行情 CSV (含 Emerging dailyDl 格式與舊版 Web CSV) 清洗並匯入 SQLite
    支援：
    - 舊版 CSV：包含 "代號","名稱","收盤","開盤" 等欄位
    - 新版 Emerging CSV：包含 TITLE,DATADATE,ALIGN,HEADER,BODY 格式，且取用 BODY
    """
    filename = os.path.basename(csv_path)

    # ── (0) 建表：保證主表存在
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                證券代號 TEXT,
                證券名稱 TEXT,
                收盤價 REAL,
                開盤價 REAL,
                最高價 REAL,
                最低價 REAL,
                成交股數 INTEGER,
                成交金額 INTEGER,
                日期 TEXT,
                PRIMARY KEY (證券代號, 日期)
            );
        """)
        conn.commit()

    # ── (1) 掃描 CSV：偵測 internal_date 與 header_row，並判斷格式
    internal_date = None
    header_row = None
    emerging_format = False
    emerging_cols = None
    emerg_date_pattern = re.compile(r"日期[:：]\s*(\d{3})年(\d{1,2})月(\d{1,2})日")
    with open(csv_path, encoding="utf-8-sig", errors="ignore") as f:
        for idx, line in enumerate(f):
            s = line.strip()
            # 檢測 Emerging 日期行
            if internal_date is None:
                m = emerg_date_pattern.search(s)
                if m:
                    ry, rm, rd = m.groups()
                    internal_date = f"{int(ry)+1911:04d}{int(rm):02d}{int(rd):02d}"
            # 偵測 Emerging HEADER 行
            if s.startswith("HEADER,"):
                header_row = idx
                emerging_format = True
                emerging_cols = s.split(",")[1:]
                break
            # 偵測舊版 Web CSV HEADER
            if s.startswith('"代號"') or s.startswith("代號"):
                header_row = idx
                emerging_format = False
                break

    if header_row is None:
        print(f"⚠️ 無法偵測到 HEADER 行，跳過 {filename}")
        return

    # ── (2) 日期校驗：檔名 vs 內部日期
    fn_date = date_str or filename.split("_")[-1].split(".")[0]
    if internal_date and internal_date != fn_date:
        print(f"⚠️ CSV 內部日期 {internal_date} 與 檔名/參數日期 {fn_date} 不符，跳過")
        return
    date_str = internal_date or fn_date

    # ── (3) 讀入資料
    if emerging_format:
        df_raw = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            skiprows=header_row,
            header=None,
            dtype=str
        )
        df_raw.columns = ["prefix"] + emerging_cols
        df = df_raw[df_raw["prefix"] == "BODY"].drop(columns=["prefix"]).copy()
    else:
        df = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            skiprows=header_row,
            dtype=str
        ).dropna(how="all")
        df.columns = df.columns.str.strip().str.replace('"', '')

    # ── (4) 欄位篩選與重命名
    if emerging_format:
        wanted = {
            "證券代號": "證券代號",
            "證券名稱": "證券名稱",
            "最後":     "收盤價",
            "最高":     "最高價",
            "最低":     "最低價",
            "成交量":   "成交股數",
            "成交金額": "成交金額",
        }
    else:
        wanted = {
            "代號":       "證券代號",
            "名稱":       "證券名稱",
            "收盤":       "收盤價",
            "開盤":       "開盤價",
            "最高":       "最高價",
            "最低":       "最低價",
            "成交股數":   "成交股數",
            "成交金額(元)": "成交金額",
        }
    missing = [k for k in wanted if k not in df.columns]
    if missing:
        print(f"⚠️ 缺少欄位 {missing}，跳過 {filename}")
        return
    df = df[list(wanted)].rename(columns=wanted)

    # 舊版若無開盤價, 則補為收盤價
    if "開盤價" not in df.columns:
        df["開盤價"] = df["收盤價"]

    # ── (5) 清理證券代號
    df["證券代號"] = (
        df["證券代號"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.strip()
    )

    # ── (6) 讀取 stock_id.csv 並過濾 (僅適用於 Web CSV，Emerging 不過濾)
    if not emerging_format:
        cur = os.path.dirname(csv_path)
        stock_id_csv = None
        while True:
            candidate = os.path.join(cur, "stock_id", "stock_id.csv")
            if os.path.exists(candidate):
                stock_id_csv = candidate
                break
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
        if stock_id_csv is None:
            raise FileNotFoundError("找不到 stock_id/stock_id.csv，請確認目錄結構")
        stock_ids_df = pd.read_csv(
            stock_id_csv, dtype=str, encoding="utf-8-sig")
        stock_ids_df.columns = stock_ids_df.columns.str.strip()
        if "證券代號" not in stock_ids_df.columns and "stock_id" in stock_ids_df.columns:
            stock_ids_df = stock_ids_df.rename(columns={"stock_id": "證券代號"})
        valid_ids = (
            stock_ids_df["證券代號"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("")
            .str.strip()
            .tolist()
        )
        before = len(df)
        df = df[df["證券代號"].isin(valid_ids)].copy()
        removed = before - len(df)
        if removed:
            print(f"⚠️ 已過濾 {removed} 筆不在 stock_id.csv 清單的資料")

    # 新版 Emerging CSV 不過濾，保留所有資料
    if df.empty:
        print(f"⚠️ {date_str} 無有效資料，跳過")
        return

    # ── (7) 數值轉型
    for col in ["收盤價", "開盤價", "最高價", "最低價"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
        )
    df["成交股數"] = (
        df["成交股數"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["成交金額"] = (
        df["成交金額"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # ── (8) 加入日期欄
    df["日期"] = date_str

    # ── (9) 寫入 SQLite
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT 1 FROM {table_name} WHERE 日期 = ? LIMIT 1",
            (date_str,)
        )
        if cur.fetchone():
            print(f"⚠️ 已存在 {date_str} 的資料，跳過")
            return
        temp = f"{table_name}_temp"
        df.to_sql(temp, conn, if_exists="replace", index=False)
        cur.execute(
            f"INSERT OR IGNORE INTO {table_name} SELECT * FROM {temp};")
        conn.commit()
        cur.execute(f"DROP TABLE IF EXISTS {temp};")
        conn.commit()
    print(f"✅ {date_str} 匯入 {table_name} 成功，共 {len(df)} 筆")
