import os
import pandas as pd
import sqlite3

def import_tpex_institutional_csv_to_sqlite(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "tpex_institutional_chip"
):
    """
    將 TPEX（三大法人）買賣超 CSV 清洗並匯入 SQLite
    csv_path: 檔案路徑，格式為 …/tpex_institutional_<ROCYYYYMMDD>.csv
    sqlite_path: SQLite DB 路徑
    table_name: 要寫入的資料表名稱
    """
    # 從檔名推日期（ROC 年份）
    filename = os.path.basename(csv_path)
    raw = filename.split("_")[-1].split(".")[0]      # e.g. "1140508"
    roc_year = int(raw[:3])
    month    = int(raw[3:5])
    day      = int(raw[5:7])
    # 轉回西元 YYYYMMDD
    date_str = f"{roc_year + 1911:04d}{month:02d}{day:02d}"

    # 讀檔（UTF-8 with BOM）
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    # 去除全空列並修剪欄名
    df = df.dropna(how="all")
    df.columns = df.columns.str.strip()

    # 只取必要欄位，並重命名成統一格式
    df = df[[
        "證券代號", "證券名稱",
        "外資_買賣超", "投信_買賣超", "自營_買賣超"
    ]].copy()
    df.columns = [
        "證券代號", "證券名稱",
        "外資買賣超", "投信買賣超", "自營買賣超"
    ]

    # 清理「證券代號」＆轉數值
    df["證券代號"] = df["證券代號"].astype(str).str.extract(r"(\d+)")
    for col in ["外資買賣超", "投信買賣超", "自營買賣超"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
            .astype(int)
        )

    # 計算三大法人合計
    df["三大法人合計"] = df[
        ["外資買賣超", "投信買賣超", "自營買賣超"]
    ].sum(axis=1)

    # 加上日期欄
    df["日期"] = date_str

    if df.empty:
        print(f"⚠️ {filename} 無有效資料，跳過")
        return

    # 確保資料夾存在
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)

    # 寫入 SQLite
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        # 建表（如尚未存在）
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                證券代號 TEXT,
                證券名稱 TEXT,
                外資買賣超 INTEGER,
                投信買賣超 INTEGER,
                自營買賣超 INTEGER,
                三大法人合計 INTEGER,
                日期 TEXT,
                PRIMARY KEY (證券代號, 日期)
            );
        """)
        conn.commit()

        # 暫存再去重覆匯入
        temp_table = f"{table_name}_temp"
        df.to_sql(temp_table, conn, if_exists="replace", index=False)

        cur.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {temp_table};
        """)
        conn.commit()

    print(f"✅ {date_str} 匯入 {table_name} 成功，共 {len(df)} 筆")
