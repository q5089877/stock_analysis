import os
import sqlite3
import pandas as pd


def import_credit_twse_sql(csv_path: str, sqlite_path: str) -> None:
    """
    讀取 TWSE 融資／融券 CSV，定位表頭並清洗資料，
    最終以 INSERT OR IGNORE 將每日資料累計匯入 SQLite。
    """
    # 1. 讀所有行，找到含「代號」和「名稱」的表頭
    with open(csv_path, encoding='utf-8-sig') as f:
        lines = f.readlines()
    header_idx = next((i for i, l in enumerate(
        lines) if '代號' in l and '名稱' in l), None)
    if header_idx is None:
        print(f"⚠️ 無法在 {csv_path} 找到表頭，跳過此檔案。")
        return

    # 2. 從表頭那行開始讀成 DataFrame
    df = pd.read_csv(csv_path, skiprows=header_idx, encoding='utf-8-sig')

    # 3. 刪掉空欄、Unnamed 欄，還有「註記」欄
    df = df.loc[:, df.columns.str.strip().astype(bool)]
    df = df.loc[:, ~df.columns.str.contains(r'^Unnamed', na=False)]
    if '註記' in df.columns:
        df = df.drop(columns=['註記'])

    # 4. 重命名欄位
    df.columns = [
        '代號', '名稱',
        '買進', '賣出', '現金償還', '前日餘額', '今日餘額', '次一營業日限額',
        '買進.1', '賣出.1', '現券償還', '前日餘額.1', '今日餘額.1', '次一營業日限額.1',
        '資券互抵'
    ]
    df = df.rename(columns={
        '代號': '證券代號',
        '名稱': '名稱',
        '前日餘額': '融資前日餘額',
        '買進': '融資買進',
        '賣出': '融資賣出',
        '現金償還': '融資現金償還',
        '今日餘額': '融資今日餘額',
        '前日餘額.1': '融券前日餘額',
        '買進.1': '融券買進',
        '賣出.1': '融券賣出',
        '現券償還': '融券現償',
        '今日餘額.1': '融券今日餘額',
        '資券互抵': '資券互抵'
    })

    # 5. 篩選需要欄位
    cols = [
        '證券代號', '名稱',
        '融資前日餘額', '融資買進', '融資賣出', '融資現金償還', '融資今日餘額',
        '融券前日餘額', '融券買進', '融券賣出', '融券現償', '融券今日餘額', '資券互抵'
    ]
    df = df[cols].copy()

    # 6. 清洗數值欄位：去掉千分號，非數字變 NaN，填 0，再轉成 int
    for c in cols[2:]:
        df[c] = (
            pd.to_numeric(
                df[c].astype(str).str.replace(',', '', regex=False),
                errors='coerce'
            )
            .fillna(0)
            .astype(int)
        )

    # 7. 篩選 stock_id.csv 內的證券
    data_dir = os.path.dirname(os.path.dirname(os.path.dirname(csv_path)))
    stock_id_csv = os.path.join(data_dir, 'stock_id', 'stock_id.csv')
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv: {stock_id_csv}")
    valid_ids = (
        pd.read_csv(stock_id_csv, dtype=str, encoding='utf-8-sig')['stock_id']
          .str.extract(r'(\d+)', expand=False).fillna('').str.strip()
    )
    df = df[df['證券代號'].isin(valid_ids)].reset_index(drop=True)

    # 8. 寫回 SQLite
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS credit_twse (
            date TEXT,
            證券代號 TEXT,
            名稱 TEXT,
            融資前日餘額 INTEGER,
            融資買進 INTEGER,
            融資賣出 INTEGER,
            融資現金償還 INTEGER,
            融資今日餘額 INTEGER,
            融券前日餘額 INTEGER,
            融券買進 INTEGER,
            融券賣出 INTEGER,
            融券現償 INTEGER,
            融券今日餘額 INTEGER,
            資券互抵 INTEGER,
            PRIMARY KEY (證券代號, date)
        )
    """)
    date = os.path.basename(csv_path).split('_')[-1].split('.')[0]
    df.insert(0, 'date', date)
    placeholders = ','.join('?' for _ in df.columns)
    sql = f"INSERT OR IGNORE INTO credit_twse VALUES ({placeholders})"
    for row in df.itertuples(index=False, name=None):
        cur.execute(sql, row)
    conn.commit()
    conn.close()

    print(f"[測試] credit_twse_{date} 匯入完成，共 {len(df)} 筆")
