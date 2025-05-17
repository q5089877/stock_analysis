import os
import sqlite3
import pandas as pd

def import_tpex_csv_to_sqlite(
    csv_path: str,
    sqlite_path: str,
    table_name: str = "tpex_chip",
    date_str: str = None
):
    """
    將 TPEx 行情 CSV 清洗並匯入 SQLite
    - 會自動偵測 header 行（含 "代號","名稱",...）
    - 跳過前面 metadata
    - date_str: YYYYMMDD（若提供，會加到 df['日期']）
    """
    # 1. 先找出 header 在第幾行
    header_row = None
    with open(csv_path, encoding="utf-8-sig") as f:
        for idx, line in enumerate(f):
            if line.strip().startswith('"代號"') or line.strip().startswith('代號'):
                header_row = idx
                break
    if header_row is None:
        print(f"⚠️ 無法偵測到 header 行，跳過 {csv_path}")
        return

    # 2. 用 pandas 讀取、跳過前 header_row 列
    df = pd.read_csv(
        csv_path,
        encoding="utf-8-sig",
        skiprows=header_row,
        dtype=str  # 先全當字串
    )

    # 3. 去除全空列與欄名空白
    df = df.dropna(how="all")
    df.columns = df.columns.str.strip().str.replace('"', '')

    # 4. 篩選並重命名欄位（以你需要的為例，可調整）
    #    確保這些欄位名稱存在於 df.columns
    wanted = {
        '代號': '證券代號',
        '名稱': '證券名稱',
        '收盤': '收盤價',
        '開盤': '開盤價',
        '最高': '最高價',
        '最低': '最低價',
        '成交股數': '成交股數',
        '成交金額(元)': '成交金額',
    }
    missing = [k for k in wanted if k not in df.columns]
    if missing:
        print(f"⚠️ 欄位 {missing} 在 {csv_path} 找不到，請檢查格式")
        return

    df = df[list(wanted.keys())].rename(columns=wanted)

    # 5. 轉換數值欄位
    for col in ['收盤價','開盤價','最高價','最低價']:
        df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
    df['成交股數']   = pd.to_numeric(df['成交股數'].str.replace(',', ''), errors='coerce').fillna(0).astype(int)
    df['成交金額']   = pd.to_numeric(df['成交金額'].str.replace(',', ''), errors='coerce').fillna(0).astype(int)

    # 6. 加入日期欄
    if date_str:
        df['日期'] = date_str
    else:
        # 從檔案名推
        fname = os.path.basename(csv_path)
        df['日期'] = fname.split('_')[-1].split('.')[0]

    # 7. 寫入 SQLite
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        cur = conn.cursor()
        # 建表
        cur.execute(f"""
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

        # 暫存導入
        temp = f"{table_name}_temp"
        df.to_sql(temp, conn, if_exists="replace", index=False)

        # 插入不重複的
        cur.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM {temp};
        """)
        conn.commit()

    print(f"✅ {date_str or ''} 匯入 {table_name} 成功，共 {len(df)} 筆")
