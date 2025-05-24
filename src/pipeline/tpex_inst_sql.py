import os
import pandas as pd
import sqlite3


def import_tpex_inst_sql(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "tpex_institutional_chip"
):
    """
    將 TPEX（三大法人）買賣超 CSV 清洗並匯入 SQLite
    功能涵蓋：
      1. 檔案大小檢查（<3KB 跳過）
      2. 主表一開始建立，避免後續跳過導致表消失
      3. 從檔名解析 ROC 年月日 → 西元 YYYYMMDD
      4. 讀 CSV、剔除全空列、欄位重命名
      5. 清理證券代號：只擷取數字、去除前後空白
      6. 讀取 stock_id.csv，僅保留在清單內的代號
      7. 外資/投信/自營 欄位轉成整數、逗號去除、空值填 0
      8. 計算「三大法人合計」並加入「日期」欄
      9. 無有效資料時：保留空表、跳過寫入；有資料時：用暫存表 + INSERT OR IGNORE 寫入
    """
    filename = os.path.basename(csv_path)

    # ─── 0. 確保主表存在 ─────────────────────────────
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(f"""
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
    # ─────────────────────────────────────────────────

    # ─── 1. 檔案大小檢查 ───────────────────────────────
    size_bytes = os.path.getsize(csv_path)
    if size_bytes < 3 * 1024:
        print(f"⚠️ 檔案 {filename} 太小（{size_bytes} bytes < 3KB），跳過匯入")
        return
    # ─────────────────────────────────────────────────

    # ─── 2. 從檔名解析日期（ROC→西元）──────────────────
    raw = filename.split("_")[-1].split(".")[0]      # e.g. "1140508"
    roc_year = int(raw[:3])
    month = int(raw[3:5])
    day = int(raw[5:7])
    date_str = f"{roc_year + 1911:04d}{month:02d}{day:02d}"
    # ─────────────────────────────────────────────────

    # ─── 3. 讀 CSV & 欄位清理 ─────────────────────────
    df = pd.read_csv(csv_path, encoding="utf-8-sig").dropna(how="all")
    df.columns = df.columns.str.strip()
    df = df[["證券代號", "證券名稱", "外資_買賣超", "投信_買賣超", "自營_買賣超"]].copy()
    df.columns = ["證券代號", "證券名稱", "外資買賣超", "投信買賣超", "自營買賣超"]
    # ─────────────────────────────────────────────────

    # ─── 4. 清理證券代號 ───────────────────────────────
    df["證券代號"] = (
        df["證券代號"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.strip()
    )
    # ─────────────────────────────────────────────────

    # ─── 5. 讀取 stock_id.csv 清單並過濾 ────────────────
    # 假設 stock_id.csv 位於 data/stock_id/ 下，csv_path 在 data/raw/... 或 data/tpex_inst/... 下
    # 因此向上追溯三層，抵達 data 資料夾
    data_dir = os.path.dirname(os.path.dirname(os.path.dirname(csv_path)))
    stock_id_csv = os.path.join(data_dir, "stock_id", "stock_id.csv")
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv，檢查路徑：{stock_id_csv}")
    stock_ids_df = pd.read_csv(stock_id_csv, dtype={"stock_id": str})
    valid_ids = (
        stock_ids_df["stock_id"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.strip()
        .tolist()
    )
    before_sid = len(df)
    df = df[df["證券代號"].isin(valid_ids)].copy()
    removed_sid = before_sid - len(df)
    if removed_sid > 0:
        print(f"⚠️ 已過濾 {removed_sid} 筆不在 stock_id.csv 清單內的資料")
    # ─────────────────────────────────────────────────

    # 若無任何有效資料 → 跳過寫入
    if df.empty:
        print(f"⚠️ {filename} 無任何符合條件的資料，僅保留空表")
        return

    # ─── 6. 數值欄位轉整數 & 填補 ───────────────────────
    for col in ["外資買賣超", "投信買賣超", "自營買賣超"]:
        df[col] = (
            df[col].astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
            .astype(int)
        )
    # ─────────────────────────────────────────────────

    # ─── 7. 計算合計 & 加入日期 ─────────────────────────
    df["三大法人合計"] = df[["外資買賣超", "投信買賣超", "自營買賣超"]].sum(axis=1)
    df["日期"] = date_str
    # ─────────────────────────────────────────────────

    # ─── 8. 寫入 SQLite（暫存表 + INSERT OR IGNORE）───────
    with sqlite3.connect(sqlite_path) as conn:
        temp = f"{table_name}_temp"
        df.to_sql(temp, conn, if_exists="replace", index=False)
        conn.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {temp};
        """)
        conn.execute(f"DROP TABLE IF EXISTS {temp};")
        conn.commit()
    # ─────────────────────────────────────────────────

    print(f"✅ {date_str} 匯入 {table_name} 成功，共 {len(df)} 筆")
