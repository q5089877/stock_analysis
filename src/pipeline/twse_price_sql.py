import os
import pandas as pd
import sqlite3
import csv
from io import StringIO


def import_twse_price_sql(csv_path: str, sqlite_path: str, table_name: str = "twse_chip", date_str: str = ""):
    """
    將 TWSE 原始 CSV 匯入 SQLite 資料庫（自動清洗、去重與追加），並加上日期欄位
    - 只匯入證券代號在 stock_id.csv 清單裡的資料
    """
    # 1. 讀檔並找出 header
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()
    header_index = next(i for i, l in enumerate(lines) if "證券代號" in l)
    header_line = lines[header_index].strip()
    data_lines = [l.strip() for l in lines[header_index + 1:] if l.strip()]

    # 2. 組成 CSV text，再用 csv.reader 解析
    csv_text = header_line + "\n" + "\n".join(data_lines)
    reader = csv.reader(StringIO(csv_text))
    records = list(reader)
    df = pd.DataFrame(records[1:], columns=records[0])

    # 3. 清掉空欄、去掉多餘欄
    df = df.loc[:, df.columns.str.strip() != ""]
    df["證券代號"] = df["證券代號"].str.extract(r"(\d+)", expand=False)

    # 4. 刪除不需要的成交揭示欄
    df = df.drop(columns=["最後揭示買價", "最後揭示買量",
                 "最後揭示賣價", "最後揭示賣量"], errors="ignore")

    # 5. 數值欄位清洗
    int_cols = ["成交股數", "成交筆數"]
    float_cols = ["成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "本益比"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col].str.replace(
            ",", ""), errors="coerce").fillna(0).astype(int)
    for col in float_cols:
        df[col] = pd.to_numeric(df[col].str.replace(
            ",", ""), errors="coerce").fillna(0.0)

    # 6. 加入日期欄
    if not date_str:
        # 如果沒傳入，就從檔名取
        date_str = os.path.basename(csv_path).split("_")[-1].split(".")[0]
    df["日期"] = date_str

    # ── 新增：讀取 stock_id.csv 並過濾 ───────────────────────────
    # 自動向上尋找 stock_id/stock_id.csv
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

    # 讀清單並標準化欄名
    stock_ids_df = pd.read_csv(stock_id_csv, dtype=str, encoding="utf-8-sig")
    stock_ids_df.columns = stock_ids_df.columns.str.strip()
    if "證券代號" not in stock_ids_df.columns and "stock_id" in stock_ids_df.columns:
        stock_ids_df = stock_ids_df.rename(columns={"stock_id": "證券代號"})
    if "證券代號" not in stock_ids_df.columns:
        raise KeyError(f"stock_id.csv 欄位錯誤：{stock_ids_df.columns.tolist()}")

    # 萃取純數字並做過濾
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
        print(f"⚠️ 已過濾掉 {removed} 筆不在 stock_id.csv 清單的資料")
    if df.empty:
        print(f"⚠️ {csv_path} 無任何符合 stock_id 清單的資料，跳過")
        return
    # ─────────────────────────────────────────────────────────────

    # 7. 建表 & 寫入 SQLite（忽略重複）
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                證券代號 TEXT,
                證券名稱 TEXT,
                成交股數 INTEGER,
                成交筆數 INTEGER,
                成交金額 REAL,
                開盤價 REAL,
                最高價 REAL,
                最低價 REAL,
                收盤價 REAL,
                漲跌 TEXT,
                漲跌價差 REAL,
                本益比 REAL,
                日期 TEXT,
                PRIMARY KEY (證券代號, 日期)
            );
        """)
        conn.commit()

        df.to_sql(table_name + "_temp", conn, if_exists="replace", index=False)
        conn.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {table_name}_temp
        """)
        conn.execute(f"DROP TABLE IF EXISTS {table_name}_temp;")
        conn.commit()

    print(f"✅ {date_str} 匯入 {table_name} 成功，共 {len(df)} 筆")
