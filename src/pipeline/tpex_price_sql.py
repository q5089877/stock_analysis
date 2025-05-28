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
    將 TPEx 行情 CSV (Emerging dailyDl 或舊版 Web CSV) 清洗並匯入 SQLite
    已移除「開盤價」欄位，僅保留 收盤、最高、最低、成交量/金額
    """
    filename = os.path.basename(csv_path)

    # ── (0) 建主表，不含開盤價
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                證券代號 TEXT,
                證券名稱 TEXT,
                收盤價   REAL,
                最高價   REAL,
                最低價   REAL,
                成交股數 INTEGER,
                成交金額 INTEGER,
                日期     TEXT,
                PRIMARY KEY (證券代號, 日期)
            );
        """)
        conn.commit()

    # ── (1) 偵測格式與 HEADER 行
    internal_date = None
    header_row = None
    emerging_format = False
    emerging_cols = None
    date_pattern = re.compile(r"日期[:：]\s*(\d{3})年(\d{1,2})月(\d{1,2})日")
    with open(csv_path, encoding="utf-8-sig", errors="ignore") as f:
        for idx, line in enumerate(f):
            txt = line.strip()
            if internal_date is None:
                m = date_pattern.search(txt)
                if m:
                    ry, rm, rd = m.groups()
                    internal_date = f"{int(ry)+1911:04d}{int(rm):02d}{int(rd):02d}"
            if txt.startswith("HEADER,"):
                header_row = idx
                emerging_format = True
                raw = txt.split(",")[1:]
                emerging_cols = [c.strip().strip('"') for c in raw]
                break
            if txt.startswith('"代號"') or txt.startswith("代號"):
                header_row = idx
                emerging_format = False
                break
    if header_row is None:
        print(f"⚠️ 無法偵測 HEADER 行，跳過 {filename}")
        return

    # ── (2) 確認日期
    fn_date = date_str or filename.split('_')[-1].split('.')[0]
    if internal_date and internal_date != fn_date:
        print(f"⚠️ CSV 內部日期 {internal_date} 與 檔名日期 {fn_date} 不符，跳過")
        return
    date_str = internal_date or fn_date

    # ── (3) 讀取資料
    if emerging_format:
        raw_df = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            skiprows=header_row+1,
            header=None,
            dtype=str
        )
        raw_df.columns = ["prefix"] + emerging_cols
        df = raw_df[raw_df["prefix"] == "BODY"].drop(columns=["prefix"]).copy()
    else:
        df = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            skiprows=header_row,
            dtype=str
        ).dropna(how="all")
        df.columns = df.columns.str.strip().str.replace('"', '')

    # ── (4) 欄位篩選與重命名（去除 開盤）
    if emerging_format:
        mapping = {
            "證券代號": "證券代號",
            "證券名稱": "證券名稱",
            "最後":    "收盤價",
            "最高":    "最高價",
            "最低":    "最低價",
            "成交量":  "成交股數",
            "成交金額": "成交金額",
        }
    else:
        mapping = {
            "代號":       "證券代號",
            "名稱":       "證券名稱",
            "收盤":       "收盤價",
            "最高":       "最高價",
            "最低":       "最低價",
            "成交股數":   "成交股數",
            "成交金額(元)": "成交金額",
        }
    missing = [k for k in mapping if k not in df.columns]
    if missing:
        print(f"⚠️ 缺少欄位 {missing}，跳過 {filename}")
        return
    df = df[list(mapping)].rename(columns=mapping)

    # ── (5) 清理證券代號
    df["證券代號"] = (
        df["證券代號"].astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.strip()
    )

    # ── (6) 過濾舊版資料
    if not emerging_format:
        base = os.path.dirname(csv_path)
        sid = None
        while True:
            path = os.path.join(base, "stock_id", "stock_id.csv")
            if os.path.exists(path):
                sid = path
                break
            nb = os.path.dirname(base)
            if nb == base:
                break
            base = nb
        if not sid:
            raise FileNotFoundError("找不到 stock_id.csv")
        sid_df = pd.read_csv(sid, dtype=str, encoding="utf-8-sig")
        sid_df.columns = sid_df.columns.str.strip()
        if "證券代號" not in sid_df.columns and "stock_id" in sid_df.columns:
            sid_df = sid_df.rename(columns={"stock_id": "證券代號"})
        ids = sid_df["證券代號"].astype(str).str.extract(
            r"(\d+)", expand=False).dropna().tolist()
        before = len(df)
        df = df[df["證券代號"].isin(ids)].copy()
        dropped = before - len(df)
        if dropped:
            print(f"⚠️ 過濾 {dropped} 筆不在清單資料")
    if df.empty:
        print(f"⚠️ {date_str} 無有效資料，跳過")
        return

    # ── (7) 數值轉型
    for col in ["收盤價", "最高價", "最低價"]:
        df[col] = pd.to_numeric(df[col].str.replace(
            ",", ""), errors="coerce").fillna(0)
    df["成交股數"] = pd.to_numeric(df["成交股數"].str.replace(
        ",", ""), errors="coerce").fillna(0).astype(int)
    df["成交金額"] = pd.to_numeric(df["成交金額"].str.replace(
        ",", ""), errors="coerce").fillna(0).astype(int)

    # ── (8) 加入日期欄位
    df["日期"] = date_str

    # ── (9) 排序欄位：無開盤價
    df = df[[
        '證券代號', '證券名稱', '收盤價',
        '最高價', '最低價', '成交股數', '成交金額', '日期'
    ]]

    # ── (10) 寫入 SQLite
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT 1 FROM {table_name} WHERE 日期=? LIMIT 1", (date_str,))
        if cur.fetchone():
            print(f"⚠️ 已存在 {date_str}，跳過")
            return
        df.to_sql(f"{table_name}_tmp", conn, if_exists="replace", index=False)
        cur.execute(
            f"INSERT OR IGNORE INTO {table_name} SELECT * FROM {table_name}_tmp;")
        conn.commit()
        cur.execute(f"DROP TABLE IF EXISTS {table_name}_tmp;")
        conn.commit()
    print(f"✅ {date_str} 匯入 {table_name} 成功，共 {len(df)} 筆")
