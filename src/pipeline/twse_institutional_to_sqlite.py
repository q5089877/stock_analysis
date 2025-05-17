import os
import pandas as pd
import sqlite3


def import_institutional_csv_to_sqlite(csv_path: str, sqlite_path: str, table_name: str = "institutional_chip"):
    """
    將 TWSE 三大法人買賣超 CSV 清洗並匯入 SQLite
    """
    # 從檔名中推斷日期
    filename = os.path.basename(csv_path)
    date_str = filename.split("_")[-1].split(".")[0]

    # 讀檔（Big5已轉為UTF-8）
    df_raw = pd.read_csv(csv_path, encoding="utf-8-sig", skiprows=1)

    # 去除空白列與全空欄位
    df_raw = df_raw.dropna(how='all')
    df_raw.columns = df_raw.columns.str.strip()

    # 過濾欄位並重新命名
    df = df_raw.loc[:, [
        "證券代號", "證券名稱",
        "外陸資買賣超股數(不含外資自營商)",
        "投信買賣超股數",
        "自營商買賣超股數",
        "三大法人買賣超股數"
    ]].copy()

    df.columns = ["證券代號", "證券名稱", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計"]

    # 清理代號與數值欄位
    df["證券代號"] = df["證券代號"].astype(str).str.extract(r'(\d+)')
    for col in ["外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(
            ",", ""), errors="coerce").fillna(0).astype(int)

    # 加入日期欄位
    df["日期"] = date_str

    if df.empty:
        print(f"⚠️ {csv_path} 無有效資料，已跳過")
        return

    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                證券代號 TEXT,
                證券名稱 TEXT,
                外資買賣超 INTEGER,
                投信買賣超 INTEGER,
                自營商買賣超 INTEGER,
                三大法人合計 INTEGER,
                日期 TEXT,
                PRIMARY KEY (證券代號, 日期)
            );
        """)
        conn.commit()

        df.to_sql(f"{table_name}_temp", conn, if_exists="replace", index=False)

        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {table_name}_temp;
        """)
        conn.commit()

    print(f"✅ {date_str} 匯入成功，共 {len(df)} 筆")
