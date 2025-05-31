import os
import sqlite3
import pandas as pd


def import_credit_tpex_sql(csv_path: str, sqlite_path: str) -> None:
    """
    讀取 TPEx (上櫃) 融資／融券 CSV，定位表頭並清洗資料，
    以 INSERT OR IGNORE 累計匯入 SQLite。
    """
    # 1. 讀取所有行並找出表頭行（包含「代號」與「名稱」）
    with open(csv_path, encoding='utf-8-sig') as f:
        lines = f.readlines()
    header_idx = next(
        (i for i, line in enumerate(lines) if '代號' in line and '名稱' in line),
        None
    )
    if header_idx is None:
        print(f"⚠️ 無法在 {csv_path} 找到表頭，跳過此檔案。")
        return

    # 2. 從表頭行讀入 DataFrame
    df = pd.read_csv(csv_path, skiprows=header_idx, encoding='utf-8-sig')

    # 3. 刪除空白和 Unnamed 欄位，移除註記
    df = df.loc[:, df.columns.str.strip().astype(bool)]
    df = df.loc[:, ~df.columns.str.contains(r'^Unnamed', na=False)]
    if '註記' in df.columns:
        df = df.drop(columns=['註記'])

    # 4. 重命名「代號」欄
    df = df.rename(columns={'代號': '證券代號'})

    # 5. 保留並統一欄位名稱
    df = df[[  # 注意這些欄位名要跟來源一致
        '證券代號', '名稱',
        '前資餘額(張)', '資買', '資賣', '現償', '資餘額',
        '前券餘額(張)', '券賣', '券買', '券償', '券餘額',
        '資券相抵(張)'
    ]].copy()

    # 6. 過濾有效證券代碼
    cur_dir = os.path.dirname(csv_path)
    stock_id_csv = None
    while True:
        candidate = os.path.join(cur_dir, 'stock_id', 'stock_id.csv')
        if os.path.exists(candidate):
            stock_id_csv = candidate
            break
        parent = os.path.dirname(cur_dir)
        if parent == cur_dir:
            break
        cur_dir = parent
    if not stock_id_csv:
        raise FileNotFoundError("找不到 stock_id/stock_id.csv，請確認目錄結構")

    valid_ids = (
        pd.read_csv(stock_id_csv, dtype=str, encoding='utf-8-sig')['stock_id']
          .str.extract(r'(\d+)', expand=False).fillna('').str.strip()
    )
    df = df[df['證券代號'].isin(valid_ids)].reset_index(drop=True)

    # 7. 清洗數值欄：去逗號 → pd.to_numeric(errors='coerce') → fillna(0) → astype(int)
    num_map = {
        '前資餘額(張)': '融資前日餘額',
        '資買':        '融資買進',
        '資賣':        '融資賣出',
        '現償':        '融資現償',
        '資餘額':      '融資餘額',
        '前券餘額(張)': '融券前日餘額',
        '券賣':        '融券賣出',
        '券買':        '融券買進',
        '券償':        '融券現償',
        '券餘額':      '融券餘額',
        '資券相抵(張)': '資券互抵'
    }
    df = df.rename(columns=num_map)
    for col in num_map.values():
        df[col] = (
            pd.to_numeric(
                df[col].astype(str).str.replace(',', '', regex=False),
                errors='coerce'
            )
            .fillna(0)
            .astype(int)
        )

    # 8. 建立 table if not exists 並 INSERT OR IGNORE
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS credit_tpex (
            date TEXT,
            證券代號 TEXT,
            名稱 TEXT,
            融資前日餘額 INTEGER,
            融資買進 INTEGER,
            融資賣出 INTEGER,
            融資現償 INTEGER,
            融資餘額 INTEGER,
            融券前日餘額 INTEGER,
            融券買進 INTEGER,
            融券賣出 INTEGER,
            融券現償 INTEGER,
            融券餘額 INTEGER,
            資券互抵 INTEGER,
            PRIMARY KEY (證券代號, date)
        )
    """)
    # 9. 插入日期欄
    date = os.path.basename(csv_path).split('_')[-1].split('.')[0]
    df.insert(0, 'date', date)

    # 10. 批次 INSERT
    cols = df.columns.tolist()
    placeholders = ','.join('?' for _ in cols)
    sql = f"INSERT OR IGNORE INTO credit_tpex ({','.join(cols)}) VALUES ({placeholders})"
    for vals in df.itertuples(index=False, name=None):
        cur.execute(sql, vals)

    conn.commit()
    conn.close()

    print(f"[測試] credit_tpex_{date} 匯入完成，共 {len(df)} 筆")


if __name__ == "__main__":
    from src.utils.config_loader import load_config
    cfg = load_config()
    sqlite_path = cfg['paths']['sqlite']
    raw_dir = cfg['paths']['raw_data']
    test_date = '20250529'
    csv_file = os.path.join(raw_dir, 'credit_tpex',
                            f'tpex_credit_{test_date}.csv')
    import_credit_tpex_sql(csv_file, sqlite_path)
