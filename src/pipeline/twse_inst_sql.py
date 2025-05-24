import os
import pandas as pd
import sqlite3


def import_inst_sql(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "twse_institutional_chip"
):
    """
    將 TWSE 三大法人買賣超 CSV 清洗並匯入 SQLite
    並且只保留 stock_id.csv 清單內的證券代號
    """
    # 1. 從檔名中推斷日期
    filename = os.path.basename(csv_path)
    date_str = filename.split("_")[-1].split(".")[0]

    # 2. 讀檔（Big5已轉為UTF-8）並基本清理
    df_raw = pd.read_csv(csv_path, encoding="utf-8-sig",
                         skiprows=1).dropna(how="all")
    df_raw.columns = df_raw.columns.str.strip()

    # 3. 過濾必要欄位並重命名
    df = df_raw.loc[:, [
        "證券代號", "證券名稱",
        "外陸資買賣超股數(不含外資自營商)",
        "投信買賣超股數",
        "自營商買賣超股數",
        "三大法人買賣超股數"
    ]].copy()
    df.columns = [
        "證券代號", "證券名稱",
        "外資買賣超", "投信買賣超",
        "自營商買賣超", "三大法人合計"
    ]

    # 4. 擷取純數字代號，並去除原始 NaN
    df["證券代號"] = df["證券代號"].astype(str).str.extract(r"(\d+)", expand=False)
    df = df.dropna(subset=["證券代號"])

    # 5. 證券名稱空值補「未知」
    df["證券名稱"] = df["證券名稱"].fillna("未知").astype(str)

    # 6. 數值欄位轉 int
    for col in ["外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計"]:
        df[col] = (
            pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
        )

    # 7. 加入日期欄
    df["日期"] = date_str

    # ─── 8. 讀取 stock_id.csv，僅保留清單內證券代號 ─────────────────
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

    stock_ids_df = pd.read_csv(stock_id_csv, dtype=str, encoding="utf-8-sig")
    stock_ids_df.columns = stock_ids_df.columns.str.strip()
    # 如果欄位名為 stock_id，改成 證券代號
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
    if removed > 0:
        print(f"⚠️ 已過濾 {removed} 筆不在 stock_id.csv 清單內的資料")
    # ────────────────────────────────────────────────────────────────

    # 9. 無效資料檢查
    if df.empty:
        print(f"⚠️ {filename} 無有效資料，已跳過")
        return

    # 10. 寫入 SQLite
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        cur.execute(f"""
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
        cur.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {table_name}_temp;
        """)
        conn.commit()
        cur.execute(f"DROP TABLE IF EXISTS {table_name}_temp;")
        conn.commit()

    print(f"✅ {date_str} 匯入成功，共 {len(df)} 筆")
