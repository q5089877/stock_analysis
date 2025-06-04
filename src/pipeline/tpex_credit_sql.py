import os
import sqlite3
import pandas as pd


def import_credit_tpex_sql(csv_path: str, sqlite_path: str) -> None:
    """
    讀取 TPEx (上櫃) 融資／融券 CSV，找到表頭並清洗資料，
    把「融資限額」與「融券限額」寫進 SQLite。
    （程式不會自己刪除舊表格，必須先手動 DROP TABLE credit_tpex 再執行）
    """

    # 1. 先把整個 CSV 的所有行讀進來，找到「代號」和「名稱」那一行是表頭
    with open(csv_path, encoding='utf-8-sig') as f:
        lines = f.readlines()
    header_idx = next(
        (i for i, line in enumerate(lines) if '代號' in line and '名稱' in line),
        None
    )
    if header_idx is None:
        print(f"⚠️ 無法在 {csv_path} 找到表頭，跳過此檔案。")
        return

    # 2. 從表頭那一行開始，用 pandas 讀成 DataFrame
    df = pd.read_csv(csv_path, skiprows=header_idx, encoding='utf-8-sig')

    # 3. 刪除空白欄、Unnamed 欄，還有「備註」欄（如果有的話）
    df = df.loc[:, df.columns.str.strip().astype(bool)]
    df = df.loc[:, ~df.columns.str.contains(r'^Unnamed', na=False)]
    if '備註' in df.columns:
        df = df.drop(columns=['備註'])

    # 4. 原始欄位如下（index 從 0 計算）：
    #  0 代號
    #  1 名稱
    #  2 前資餘額(張)
    #  3 資買
    #  4 資賣
    #  5 現償
    #  6 資餘額
    #  7 資屬證金
    #  8 資使用率(%)
    #  9 資限額
    # 10 前券餘額(張)
    # 11 券賣
    # 12 券買
    # 13 券償
    # 14 券餘額
    # 15 券屬證金
    # 16 券使用率(%)
    # 17 券限額
    # 18 資券相抵(張)
    # 19 備註

    # 我們只需要以下欄位：
    df = df.iloc[:, [
        0,   # 代號
        1,   # 名稱
        2,   # 前資餘額(張)
        3,   # 資買
        4,   # 資賣
        5,   # 現償
        6,   # 資餘額
        9,   # 資限額
        10,  # 前券餘額(張)
        12,  # 券買
        11,  # 券賣
        13,  # 券償
        14,  # 券餘額
        17,  # 券限額
        18   # 資券相抵(張)
    ]].copy()

    # 5. 重新命名成我們要的中文欄位
    df.columns = [
        '證券代號',
        '名稱',
        '融資前日餘額',
        '融資買進',
        '融資賣出',
        '融資現金償還',
        '融資今日餘額',
        '融資限額',
        '融券前日餘額',
        '融券買進',
        '融券賣出',
        '融券現償',
        '融券今日餘額',
        '融券限額',
        '資券相抵'
    ]

    # 6. 把所有數字欄位的逗號去掉，再轉成 int
    for c in [
        '融資前日餘額', '融資買進', '融資賣出', '融資現金償還', '融資今日餘額', '融資限額',
        '融券前日餘額', '融券買進', '融券賣出', '融券現償', '融券今日餘額', '融券限額',
        '資券相抵'
    ]:
        df[c] = (
            pd.to_numeric(
                df[c].astype(str).str.replace(',', '', regex=False),
                errors='coerce'
            )
            .fillna(0)
            .astype(int)
        )

    # 7. 只匯入 data/stock_id/stock_id.csv 裡面有的股票
    #    假設 stock_id.csv 在 <proj_root>/data/stock_id/stock_id.csv
    proj_root = os.path.abspath(os.path.join(
        os.path.dirname(__file__), os.pardir, os.pardir))
    stock_id_csv = os.path.join(proj_root, 'data', 'stock_id', 'stock_id.csv')
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv: {stock_id_csv}")
    valid_ids = (
        pd.read_csv(stock_id_csv, dtype=str, encoding='utf-8-sig')['stock_id']
          .str.extract(r'(\d+)', expand=False).fillna('').str.strip()
    )
    df = df[df['證券代號'].isin(valid_ids)].reset_index(drop=True)

    # 8. 連到 SQLite，如果表格不存在就建立
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
            融資現金償還 INTEGER,
            融資今日餘額 INTEGER,
            融資限額 INTEGER,
            融券前日餘額 INTEGER,
            融券買進 INTEGER,
            融券賣出 INTEGER,
            融券現償 INTEGER,
            融券今日餘額 INTEGER,
            融券限額 INTEGER,
            資券相抵 INTEGER,
            PRIMARY KEY (證券代號, date)
        )
    """)

    # 9. 從檔名取得日期 (csv_path 形如 .../tpex_credit_20250529.csv)
    date = os.path.basename(csv_path).split('_')[-1].split('.')[0]
    df.insert(0, 'date', date)

    # 10. 把每一列資料都 INSERT 進去
    placeholders = ','.join('?' for _ in df.columns)
    sql = f"INSERT OR IGNORE INTO credit_tpex VALUES ({placeholders})"
    for row in df.itertuples(index=False, name=None):
        cur.execute(sql, row)

    conn.commit()
    conn.close()

    print(f"[完成] credit_tpex_{date} 匯入完成，共 {len(df)} 筆")


if __name__ == "__main__":
    # 以下路徑請改成你自己專案的真實位置：
    #   假設 CSV 放在：C:/Users/q5089/Desktop/stock_analysis/data/raw/credit_tpex/tpex_credit_20250529.csv
    #   假設 SQLite 檔案放在：C:/Users/q5089/Desktop/stock_analysis/db/stockDB.db

    proj_root = os.path.abspath(os.path.join(
        os.path.dirname(__file__), os.pardir, os.pardir))
    csv_file = os.path.join(proj_root, "data", "raw",
                            "credit_tpex", "tpex_credit_20250529.csv")
    sqlite_file = os.path.join(proj_root, "db", "stockDB.db")

    import_credit_tpex_sql(csv_file, sqlite_file)
