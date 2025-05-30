# src/pipeline/ticket_sql.py

import os
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# 欄位對應（已去除股票名稱與備註）
COLUMNS = [
    "股票代號",
    "融券前日餘額", "融券賣出", "融券買進", "融券現券", "融券當日餘額", "融券限額",
    "借券前日餘額", "借券賣出", "借券還券", "借券調整", "借券當日餘額", "借券限額"
]


def parse_html(html: str) -> list[dict]:
    """
    解析 HTML 表格，跳過「股票名稱」和「備註」欄，對應到 COLUMNS
    """
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:
        return []
    tbody = table.find('tbody')
    if not tbody:
        return []

    records = []
    for tr in tbody.find_all('tr'):
        cells = [td.get_text(strip=True).replace(',', '')
                 for td in tr.find_all('td')]
        # 原表格 columns: [stock_id, stock_name, ...14 fields..., remark]
        # 總共 15 cells，COLUMNS 長度為 13，需要跳過 index=1 和最後一個
        if len(cells) < len(COLUMNS) + 2:
            continue
        # 篩選：取 cells[0] + cells[2:-1]
        values = [cells[0]] + cells[2:-1]
        record = {col: values[i] for i, col in enumerate(COLUMNS)}
        records.append(record)
    return records


def import_ticket_twse_sql(html_path: str, sqlite_path: str) -> None:
    """
    解析並匯入 TWSE 融券/借券到 ticket_twse
    """
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    rows = parse_html(html)
    if not rows:
        print(f"⚠️ 無法解析或無資料：{html_path}")
        return

    # 過濾 stock_id.csv
    data_dir = os.path.dirname(os.path.dirname(os.path.dirname(html_path)))
    stock_id_csv = os.path.join(data_dir, 'stock_id', 'stock_id.csv')
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv: {stock_id_csv}")
    ids = (
        pd.read_csv(stock_id_csv, dtype={"stock_id": str})["stock_id"]
        .astype(str).str.extract(r"(\d+)")[0].fillna("").str.strip()
    )
    df = pd.DataFrame(rows)
    before = len(df)
    df = df[df["股票代號"].isin(ids)].copy()
    removed = before - len(df)
    if removed > 0:
        print(f"⚠️ 已過濾 {removed} 筆不在 stock_id.csv 清單內的資料 TWSE")
    rows = df.to_dict('records')

    # 解析日期
    date_str = os.path.basename(html_path).split('_')[-1].split('.')[0]

    # 建立資料表
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ticket_twse")
    cur.execute(
        f"CREATE TABLE ticket_twse (date TEXT, {', '.join([col + ' INTEGER' if col != '股票代號' else col + ' TEXT' for col in COLUMNS])})")

    placeholders = ",".join(["?" for _ in range(len(COLUMNS) + 1)])
    for row in rows:
        vals = [date_str] + [int(row[col]) if row[col].isdigit()
                             else 0 for col in COLUMNS[1:]]
        # 第一欄股票代號保留字串
        vals.insert(1, row['股票代號'])
        cur.execute(f"INSERT INTO ticket_twse VALUES ({placeholders})", vals)
    conn.commit()
    conn.close()


def import_ticket_tpex_sql(html_path: str, sqlite_path: str) -> None:
    """
    解析並匯入 TPEX 融券/借券到 ticket_tpex
    """
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    rows = parse_html(html)
    if not rows:
        print(f"⚠️ 無法解析或無資料：{html_path}")
        return

    # 過濾 stock_id.csv
    data_dir = os.path.dirname(os.path.dirname(os.path.dirname(html_path)))
    stock_id_csv = os.path.join(data_dir, 'stock_id', 'stock_id.csv')
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv: {stock_id_csv}")
    ids = (
        pd.read_csv(stock_id_csv, dtype={"stock_id": str})["stock_id"]
        .astype(str).str.extract(r"(\d+)")[0].fillna("").str.strip()
    )
    df = pd.DataFrame(rows)
    before = len(df)
    df = df[df["股票代號"].isin(ids)].copy()
    removed = before - len(df)
    if removed > 0:
        print(f"⚠️ 已過濾 {removed} 筆不在 stock_id.csv 清單內的資料 TPEX")
    rows = df.to_dict('records')

    date_str = os.path.basename(html_path).split('_')[-1].split('.')[0]

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ticket_tpex")
    cur.execute(
        f"CREATE TABLE ticket_tpex (date TEXT, {', '.join([col + ' INTEGER' if col != '股票代號' else col + ' TEXT' for col in COLUMNS])})")

    placeholders = ",".join(["?" for _ in range(len(COLUMNS) + 1)])
    for row in rows:
        vals = [date_str] + [int(row[col]) if row[col].isdigit()
                             else 0 for col in COLUMNS[1:]]
        vals.insert(1, row['股票代號'])
        cur.execute(f"INSERT INTO ticket_tpex VALUES ({placeholders})", vals)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # 自我測試
    from src.utils.config_loader import load_config
    cfg = load_config()
    sqlite_path = cfg["paths"]["sqlite"]
    raw_dir = cfg["paths"]["raw_data"]

    # 測試 TWSE
    d1 = "20250522"
    p1 = os.path.join(raw_dir, "ticket_twse", f"ticket_twse_{d1}.html")
    import_ticket_twse_sql(p1, sqlite_path)
    print(f"[測試] ticket_twse_{d1} 匯入完成")

    # 測試 TPEX
    d2 = "20250528"
    p2 = os.path.join(raw_dir, "ticket_tpex", f"ticket_tpex_{d2}.html")
    import_ticket_tpex_sql(p2, sqlite_path)
    print(f"[測試] ticket_tpex_{d2} 匯入完成")
