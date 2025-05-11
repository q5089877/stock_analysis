import os
import pandas as pd
import sqlite3
import csv
from io import StringIO


def import_twse_csv_to_sqlite(csv_path: str, sqlite_path: str, table_name: str = "twse_chip", date_str: str = ""):
    """
    將 TWSE 原始 CSV 匯入 SQLite 資料庫（自動清洗、去重與追加），並加上日期欄位
    """
    # 讀檔
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    # 找出欄位列
    header_index = next(i for i, line in enumerate(lines) if "證券代號" in line)
    header_line = lines[header_index].strip()
    data_lines = [line.strip()
                  for line in lines[header_index + 1:] if line.strip()]

    # 用 csv.reader 處理欄位與資料列
    csv_text = header_line + "\n" + "\n".join(data_lines)
    reader = csv.reader(StringIO(csv_text))
    records = list(reader)
    columns = records[0]
    rows = records[1:]
    df = pd.DataFrame(rows, columns=columns)

    # 清除空欄位與多餘欄
    df = df.loc[:, df.columns.str.strip() != ""]
    df["證券代號"] = df["證券代號"].str.extract(r'(\d+)')
    df = df.drop(columns=["最後揭示買價", "最後揭示買量",
                 "最後揭示賣價", "最後揭示賣量"], errors="ignore")

    # 數值欄位清洗
    int_cols = ["成交股數", "成交筆數"]
    float_cols = ["成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "本益比"]

    for col in int_cols:
        df[col] = pd.to_numeric(df[col].str.replace(
            ",", ""), errors="coerce").fillna(0).astype(int)

    for col in float_cols:
        df[col] = pd.to_numeric(df[col].str.replace(
            ",", ""), errors="coerce").fillna(0.0)

    # 過濾掉空資料列
    df = df[df["證券代號"].notnull() & df["成交股數"].notnull()]

    # 加上日期欄位
    df["日期"] = date_str

    if df.empty:
        print(f"⚠️ {csv_path} 沒有有效資料，已跳過")
        return

    # 確保資料夾存在
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    # 寫入 SQLite（使用 append）
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
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

        # 嘗試插入，忽略重複（使用 INSERT OR IGNORE）
        df.to_sql(table_name + "_temp", conn, if_exists="replace", index=False)

        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {table_name}_temp
        """)
        conn.commit()

    print(f"✅ {date_str} 匯入成功：{len(df)} 筆")
