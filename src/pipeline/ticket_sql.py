# === src/pipeline/ticket_sql.py ===

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
    解析 HTML 表格，跳過「股票名稱」和「備註」欄，對應到 COLUMNS。
    但這裡要做一個小修正：把 HTML 上的「借券限額」當成「借券當日餘額」來用。
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
        # 先把 <td> 文字取出並去掉千分位逗號
        cells = [td.get_text(strip=True).replace(',', '')
                 for td in tr.find_all('td')]
        # 計算：我們的 COLUMNS（13 欄）對應到 HTML 實際的數值欄需要 cells 最少 14 個元素
        if len(cells) < len(COLUMNS) + 1:
            continue

        # 1. 把「股票代號」先存起來
        record = {
            "股票代號": cells[0].zfill(4),  # 不足 4 碼時補零
        }
        # 2. 把 cells[2], cells[3], ... 依次對應到 COLUMNS[1:]
        for idx, col in enumerate(COLUMNS[1:], start=1):
            raw_val = cells[idx + 1]
            record[col] = int(raw_val) if str(raw_val).isdigit() else 0

        # ====== 在這邊加兩行，把「借券限額」（HTML 的 cells[13]）塞到「借券當日餘額」裡 ======
        # COLUMNS 列表中，"借券當日餘額" 的 index 是 11；"借券限額" 的 index 是 12
        # record["借券限額"] 目前剛好就是 HTML cells[13]（真實要的當日餘額）
        # 所以把它複製到 record["借券當日餘額"]
        record["借券當日餘額"] = record["借券限額"]
        # 如果以後不想保留原本那個欄位，可以把它設成 0，或乾脆不管也沒關係
        # record["借券限額"] = 0
        # =============================================================================

        records.append(record)

    return records


def import_ticket_twse_sql(html_path: str, sqlite_path: str) -> None:
    """
    解析並匯入 TWSE 融券/借券到 ticket_twse。
    已修改：不再 DROP TABLE，每筆資料只 insert 一次 (同一天同股票不重複)。
    並且直接以固定路徑 'data/stock_id/stock_id.csv' 去讀 stock_id.csv。
    """
    # 1. 先把 HTML 讀進來
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    rows = parse_html(html)
    if not rows:
        print(f"⚠️ 無法解析或無資料：{html_path}")
        return

    # 2. 過濾 stock_id.csv: 直接讀 'data/stock_id/stock_id.csv'
    stock_id_csv = os.path.join("data", "stock_id", "stock_id.csv")
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv: {stock_id_csv}")

    ids = (
        pd.read_csv(stock_id_csv, dtype={"stock_id": str})["stock_id"]
        .astype(str).str.extract(r"(\d+)")[0].fillna("").str.strip()
    )
    df = pd.DataFrame(rows)
    df = df[df["股票代號"].isin(ids)].copy()
    if df.empty:
        print(f"⚠️ 無任何符合 stock_id.csv 清單的資料：{html_path}")
        return

    # 3. 取得當天的日期 (檔名格式 ticket_twse_YYYYMMDD.html)
    date_str = os.path.basename(html_path).split('_')[-1].split('.')[0]

    # 4. 建立資料表 ticket_twse（如果不存在就建立）
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    # 只在沒有這張表的時候 create
    cur.execute(
        "CREATE TABLE IF NOT EXISTS ticket_twse ("
        "date TEXT, "
        + ", ".join([
            f"'{col}' INTEGER" if col != "股票代號" else "'股票代號' TEXT"
            for col in COLUMNS
        ])
        + ")"
    )

    # 5. 每一筆資料插入前，先檢查是否已經存在相同 (date, 股票代號) 的紀錄
    placeholders = ",".join(["?"] * (len(COLUMNS) + 1)
                            )  # date + len(COLUMNS) 欄位
    for _, row in df.iterrows():
        # 查詢 (date, 股票代號) 是否已存在
        cur.execute(
            "SELECT 1 FROM ticket_twse WHERE date = ? AND 股票代號 = ? LIMIT 1",
            (date_str, row["股票代號"])
        )
        if cur.fetchone():
            # 如果已經有這一天、這檔股票的紀錄，就跳過 INSERT
            continue

        # 要插入的欄位順序： date, 股票代號, COLUMNS[1:], ...
        vals = [date_str, row["股票代號"]]
        for col in COLUMNS[1:]:
            # record["借券當日餘額"] 已經被塞成「真正的」借券餘額
            vals.append(int(row[col]) if str(row[col]).isdigit() else 0)

        cur.execute(
            f"INSERT INTO ticket_twse VALUES ({placeholders})", tuple(vals)
        )

    conn.commit()
    conn.close()


def import_ticket_tpex_sql(html_path: str, sqlite_path: str) -> None:
    """
    解析並匯入 TPEx 融券/借券到 ticket_tpex。
    已修改：不再 DROP TABLE，每筆資料只 insert 一次 (同一天同股票不重複)。
    並且直接以固定路徑 'data/stock_id/stock_id.csv' 去讀 stock_id.csv。
    """
    # 步驟同上，把 parse_html 的結果用在 ticket_tpex
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    rows = parse_html(html)
    if not rows:
        print(f"⚠️ 無法解析或無資料：{html_path}")
        return

    stock_id_csv = os.path.join("data", "stock_id", "stock_id.csv")
    if not os.path.exists(stock_id_csv):
        raise FileNotFoundError(f"找不到 stock_id.csv: {stock_id_csv}")

    ids = (
        pd.read_csv(stock_id_csv, dtype={"stock_id": str})["stock_id"]
        .astype(str).str.extract(r"(\d+)")[0].fillna("").str.strip()
    )
    df = pd.DataFrame(rows)
    df = df[df["股票代號"].isin(ids)].copy()
    if df.empty:
        print(f"⚠️ 無任何符合 stock_id.csv 清單的資料：{html_path}")
        return

    date_str = os.path.basename(html_path).split('_')[-1].split('.')[0]

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS ticket_tpex ("
        "date TEXT, "
        + ", ".join([
            f"'{col}' INTEGER" if col != "股票代號" else "'股票代號' TEXT"
            for col in COLUMNS
        ])
        + ")"
    )

    placeholders = ",".join(["?"] * (len(COLUMNS) + 1))
    for _, row in df.iterrows():
        cur.execute(
            "SELECT 1 FROM ticket_tpex WHERE date = ? AND 股票代號 = ? LIMIT 1",
            (date_str, row["股票代號"])
        )
        if cur.fetchone():
            continue

        vals = [date_str, row["股票代號"]]
        for col in COLUMNS[1:]:
            vals.append(int(row[col]) if str(row[col]).isdigit() else 0)

        cur.execute(
            f"INSERT INTO ticket_tpex VALUES ({placeholders})", tuple(vals)
        )

    conn.commit()
    conn.close()
