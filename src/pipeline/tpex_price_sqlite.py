import os
import re
import sqlite3
import pandas as pd


def import_tpex_price_sql(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "tpex_chip",
    date_str: str = ''
):
    """
    將 TPEx 行情 CSV 清洗並匯入 SQLite
    - 自動偵測 header 行
    - 跳過前面 metadata
    - date_str: YYYYMMDD（若提供，會取代檔名日期）
    - 只匯入證券代號在 stock_id.csv 清單裡的資料
    - 若 CSV 內部標頭的「資料日期」與檔名/參數 date_str 不符，則跳過
    - 若 SQLite 已存在該日期資料，也跳過
    """
    filename = os.path.basename(csv_path)

    # ── (0) 建表：保證主表存在 ─────────────────────────
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

    # ── 1. 掃描 CSV：抓 internal_date 與 header_row ───────────
    internal_date = None
    header_row = None
    date_pattern = re.compile(r"^資料日期[:：]\s*(\d{3})/(\d{1,2})/(\d{1,2})")
    with open(csv_path, encoding="utf-8-sig") as f:
        for idx, line in enumerate(f):
            s = line.strip()
            if internal_date is None:
                m = date_pattern.match(s)
                if m:
                    ry, rm, rd = m.groups()
                    internal_date = f"{int(ry)+1911:04d}{int(rm):02d}{int(rd):02d}"
            if s.startswith('"代號"') or s.startswith("代號"):
                header_row = idx
                break

    if header_row is None:
        print(f"⚠️ 無法偵測到 header 行，跳過 {filename}")
        return

    # ── 2. 比對檔名日期 vs 內部資料日期 ────────────────────────
    fn_date = date_str or filename.split("_")[-1].split(".")[0]
    if internal_date and internal_date != fn_date:
        print(f"⚠️ CSV 內部日期 {internal_date} 與 檔名/參數日期 {fn_date} 不符，跳過")
        return
    date_str = internal_date or fn_date

    # ── 3. 讀入資料 ────────────────────────────────────────
    df = pd.read_csv(
        csv_path,
        encoding="utf-8-sig",
        skiprows=header_row,
        dtype=str
    ).dropna(how="all")
    df.columns = df.columns.str.strip().str.replace('"', '')

    # ── 4. 欄位篩選與重命名 ─────────────────────────────────
    wanted = {
        "代號": "證券代號",
        "名稱": "證券名稱",
        "收盤": "收盤價",
        "開盤": "開盤價",
        "最高": "最高價",
        "最低": "最低價",
        "成交股數": "成交股數",
        "成交金額(元)": "成交金額",
    }
    missing = [k for k in wanted if k not in df.columns]
    if missing:
        print(f"⚠️ 缺少欄位 {missing}，跳過 {filename}")
        return
    df = df[list(wanted)].rename(columns=wanted)

    # ── 5. 清理證券代號 ───────────────────────────────────────
    df["證券代號"] = (
        df["證券代號"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.strip()
    )

    # ── 6. 讀取 stock_id.csv 並過濾 ─────────────────────────
    # 自動向上找尋 stock_id/stock_id.csv
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

    stock_ids_df = pd.read_csv(stock_id_csv, dtype=str, encoding="utf-8-sig")
    stock_ids_df.columns = stock_ids_df.columns.str.strip()
    # 如果原檔是 stock_id 而非 證券代號，就改名
    if "證券代號" not in stock_ids_df.columns and "stock_id" in stock_ids_df.columns:
        stock_ids_df = stock_ids_df.rename(columns={"stock_id": "證券代號"})

    if "證券代號" not in stock_ids_df.columns:
        raise KeyError(f"stock_id.csv 欄位錯誤：{stock_ids_df.columns.tolist()}")

    valid_ids = (
        stock_ids_df["證券代號"]
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

    # ── 7. 無效資料跳過 ───────────────────────────────────────
    if df.empty:
        print(f"⚠️ {date_str} 無有效資料，跳過")
        return

    # ── 8. 價格 & 成交數值轉型 ─────────────────────────────────
    for col in ["收盤價", "開盤價", "最高價", "最低價"]:
        df[col] = (
            df[col]
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
        )
    df["成交股數"] = (
        df["成交股數"]
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["成交金額"] = (
        df["成交金額"]
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # ── 9. 加入日期欄 ───────────────────────────────────────
    df["日期"] = date_str

    # ── 10. 寫入 SQLite（重複檢查）─────────────────────────
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        # 已經有同日期就跳過
        cur.execute(
            f"SELECT 1 FROM {table_name} WHERE 日期 = ? LIMIT 1",
            (date_str,)
        )
        if cur.fetchone():
            print(f"⚠️ 已存在 {date_str} 的資料，跳過")
            return

        temp = f"{table_name}_temp"
        df.to_sql(temp, conn, if_exists="replace", index=False)
        cur.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {temp};
        """)
        conn.commit()
        cur.execute(f"DROP TABLE IF EXISTS {temp};")
        conn.commit()

    print(f"✅ {date_str} 匯入 {table_name} 成功，共 {len(df)} 筆")
