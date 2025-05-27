import os
import sqlite3
import pandas as pd


def import_tpex_yield_sql(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "tpex_yield_pb"
):
    """
    從上櫃殖利率／PB／本益比的 CSV 建立 SQLite 資料表。
    若表已存在，先 DROP 再 CREATE。
    """
    # 1. 讀 CSV
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.replace("\ufeff", "")

    # 2. 重新命名欄位（對應 CSV 原欄位名稱）
    rename_map = {
        "殖利率(%)": "殖利率",
        "股價淨值比": "股價淨值比",
        "本益比": "本益比",
        "股票代號": "證券代號",
        "公司名稱": "證券名稱"
    }
    df = df.rename(columns=rename_map)

    # 3. 篩選所需欄位
    keep_cols = ["證券代號", "證券名稱", "殖利率", "本益比", "股價淨值比"]
    df = df[[col for col in keep_cols if col in df.columns]]

    # 4. 新增日期欄（從檔名自動判斷）
    date = os.path.basename(csv_path).split("_")[-1].split(".")[0]
    df.insert(0, "日期", date)

    # 5. 型別轉換與清洗
    for col in ["殖利率", "本益比", "股價淨值比"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce"
            ).fillna(0.0)

    df = df[df["證券代號"].notna()]  # 避免空值匯入 PRIMARY KEY

    # 6. 建表與匯入
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")

        # 建立表結構
        columns_sql = """
            日期        TEXT,
            證券代號    TEXT,
            證券名稱    TEXT,
        """
        if "殖利率" in df.columns:
            columns_sql += "殖利率      REAL,\n"
        if "本益比" in df.columns:
            columns_sql += "本益比      REAL,\n"
        if "股價淨值比" in df.columns:
            columns_sql += "股價淨值比  REAL,\n"

        columns_sql += "PRIMARY KEY(證券代號, 日期)"

        conn.execute(f"CREATE TABLE {table_name} ({columns_sql});")
        df.to_sql(table_name, conn, if_exists="append", index=False)

    print(f"✅ 新表 {table_name} 已建立並匯入 {len(df)} 筆資料")
