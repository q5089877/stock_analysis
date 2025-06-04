import os
import sqlite3
import pandas as pd


def import_credit_twse_sql(csv_path: str, sqlite_path: str) -> None:
    """
    讀取 TWSE 融資／融券 CSV，找到表頭並清洗資料，
    把「融資限額」和「融券限額」寫進 SQLite。
    （程式不會自己刪除舊表格，必須先手動 DROP TABLE 再執行）
    """

    # 1. 先把整個 CSV 的所有行讀進來，找到「代號」和「名稱」那一行是表頭
    with open(csv_path, encoding='utf-8-sig') as f:
        lines = f.readlines()
    header_idx = next((i for i, l in enumerate(
        lines) if '代號' in l and '名稱' in l), None)
    if header_idx is None:
        print(f"⚠️ 無法在 {csv_path} 找到表頭，跳過此檔案。")
        return

    # 2. 從表頭那一行開始，用 pandas 讀成 DataFrame
    df = pd.read_csv(csv_path, skiprows=header_idx, encoding='utf-8-sig')

    # 3. 刪除空白欄、Unnamed 欄，還有「註記」欄（如果有的話）
    df = df.loc[:, df.columns.str.strip().astype(bool)]
    df = df.loc[:, ~df.columns.str.contains(r'^Unnamed', na=False)]
    if '註記' in df.columns:
        df = df.drop(columns=['註記'])

    # 4. 先把每個欄位換成我們要的中文名稱
    #    原本 CSV 裡「次一營業日限額」是融資限額、「次一營業日限額.1」是融券限額
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
        '次一營業日限額': '融資限額',            # 把「次一營業日限額」改成「融資限額」
        '前日餘額.1': '融券前日餘額',
        '買進.1': '融券買進',
        '賣出.1': '融券賣出',
        '現券償還': '融券現償',
        '今日餘額.1': '融券今日餘額',
        '次一營業日限額.1': '融券限額',         # 把「次一營業日限額.1」改成「融券限額」
        '資券互抵': '資券互抵'
    })

    # 5. 篩選我們要的欄位 (現在多了融資限額、融券限額兩欄)
    cols = [
        '證券代號', '名稱',
        '融資前日餘額', '融資買進', '融資賣出', '融資現金償還', '融資今日餘額', '融資限額',
        '融券前日餘額', '融券買進', '融券賣出', '融券現償', '融券今日餘額', '融券限額',
        '資券互抵'
    ]
    df = df[cols].copy()

    # 6. 把所有數字欄位的逗號去掉，再轉成 int
    for c in cols[2:]:
        df[c] = (
            pd.to_numeric(
                df[c].astype(str).str.replace(',', '', regex=False),
                errors='coerce'
            )
            .fillna(0)
            .astype(int)
        )

    # 7. 我們只想匯入 data/stock_id/stock_id.csv 裡面有的股票
    #    由於 stock_id.csv 的路徑是 <proj_root>/data/stock_id/stock_id.csv
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

    # 如果表格還沒存在，就用 16 欄建立 credit_twse
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
            融資限額 INTEGER,
            融券前日餘額 INTEGER,
            融券買進 INTEGER,
            融券賣出 INTEGER,
            融券現償 INTEGER,
            融券今日餘額 INTEGER,
            融券限額 INTEGER,
            資券互抵 INTEGER,
            PRIMARY KEY (證券代號, date)
        )
    """)

    # 9. 從檔名取得日期 (csv_path 形如 .../twse_credit_20230525.csv)
    date = os.path.basename(csv_path).split('_')[-1].split('.')[0]
    df.insert(0, 'date', date)

    # 10. 把每一列資料都 INSERT 進去
    placeholders = ','.join('?' for _ in df.columns)
    sql = f"INSERT OR IGNORE INTO credit_twse VALUES ({placeholders})"
    for row in df.itertuples(index=False, name=None):
        cur.execute(sql, row)

    conn.commit()
    conn.close()

    print(f"[完成] credit_twse_{date} 匯入完成，共 {len(df)} 筆")


if __name__ == "__main__":
    # 以下路徑請改成你自己專案的真實位置：
    #   假設 CSV 放在：C:/Users/q5089/Desktop/stock_analysis/data/raw/credit_twse/twse_credit_20230525.csv
    #   假設 SQLite 檔案放在：C:/Users/q5089/Desktop/stock_analysis/db/stockDB.db

    proj_root = os.path.abspath(os.path.join(
        os.path.dirname(__file__), os.pardir, os.pardir))
    csv_file = os.path.join(proj_root, "data", "raw",
                            "credit_twse", "twse_credit_20230525.csv")
    sqlite_file = os.path.join(proj_root, "db", "stockDB.db")

    import_credit_twse_sql(csv_file, sqlite_file)
