import os
import time
import sqlite3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime

DB_PATH = "db/stockDB.db"
CSV_PATH = "data/stock_id/stock_id.csv"


def ensure_quarterly_table():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS quarterly_income_statement (
        stock_id TEXT,
        quarter TEXT,
        revenue INTEGER,
        cost INTEGER,
        gross_profit INTEGER,
        operating_profit INTEGER,
        pretax_profit INTEGER,
        net_profit INTEGER,
        eps REAL,
        last_update TEXT,
        PRIMARY KEY (stock_id, quarter)
    )
    """)
    conn.commit()
    conn.close()

# 判斷目前可查詢的最新季別


def get_latest_published_quarter():
    now = datetime.now()
    y = now.year
    m = now.month
    d = now.day
    twy = y - 1911

    if (m < 5) or (m == 5 and d < 15):
        return f"{twy-1}.4Q"
    elif (m < 8) or (m == 8 and d < 14):
        return f"{twy}.1Q"
    elif (m < 11) or (m == 11 and d < 14):
        return f"{twy}.2Q"
    else:
        return f"{twy}.3Q"


def fetch_quarterly_table(stock_id):
    url = f"https://concords.moneydj.com/z/zc/zce/zce_{stock_id}.djhtm"
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(0.5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table", {"id": "oMainTable"})
    driver.quit()
    if table is None:
        print(f"{stock_id}: 找不到 oMainTable")
        return None

    headers = [
        "quarter", "revenue", "cost", "gross_profit", "gross_margin",
        "operating_profit", "operating_margin", "non_operating",
        "pretax_profit", "net_profit", "eps"
    ]
    rows = []
    for tr in table.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if tds and len(tds) == 11 and "Q" in tds[0]:
            rows.append(tds)
    if not rows:
        print(f"{stock_id}: 沒有解析到季度數據！")
        return None
    df = pd.DataFrame(rows, columns=headers)

    # 數值轉型
    # 數值轉型
    for col in ["revenue", "cost", "gross_profit", "operating_profit", "pretax_profit", "net_profit"]:
        df[col] = pd.to_numeric(df[col].str.replace(",", "").replace(
            "-", "0"), errors='coerce').fillna(0).astype(int)
    df["eps"] = pd.to_numeric(df["eps"].str.replace(
        ",", "").replace("-", "0"), errors='coerce').fillna(0)

    df["quarter"] = df["quarter"].str.strip()
    df["stock_id"] = stock_id
    df["last_update"] = datetime.now().strftime("%Y-%m-%d")

    return df[["stock_id", "quarter", "revenue", "cost", "gross_profit", "operating_profit",
               "pretax_profit", "net_profit", "eps", "last_update"]]


def update_quarterly_financials():
    ensure_quarterly_table()
    latest_quarter = get_latest_published_quarter()
    print(f"目前應抓取最新季別：{latest_quarter}")

    try:
        df_csv = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df_csv = pd.read_csv(CSV_PATH, encoding="cp950")
    if 'note' not in df_csv.columns:
        df_csv['note'] = ''
    stock_ids = df_csv['stock_id'].astype(str).tolist()
    conn = sqlite3.connect(DB_PATH)
    for sid in stock_ids:
        print(f"\n== {sid} ==")
        cursor = conn.execute(
            "SELECT quarter FROM quarterly_income_statement WHERE stock_id=?", (sid,))
        existing_quarters = set(row[0] for row in cursor.fetchall())
        # 如果SQL已有最新季別，就跳過
        if latest_quarter in existing_quarters:
            msg = f'最新{latest_quarter}已在SQL'
            print(f"{sid}: {msg}")
            df_csv.loc[df_csv['stock_id'] == sid, 'note'] = msg
            continue

        df = fetch_quarterly_table(sid)
        if df is None:
            msg = '無法讀取財報'
            print(f"{sid}: {msg}")
            df_csv.loc[df_csv['stock_id'] == sid, 'note'] = msg
            continue
        new_df = df[~df['quarter'].isin(existing_quarters)]
        if new_df.empty:
            msg = '資料已存在'
        else:
            msg = f"成功寫入{len(new_df)}筆"
            new_df.to_sql("quarterly_income_statement", conn,
                          if_exists="append", index=False)
        print(f"{sid}: {msg}")
        df_csv.loc[df_csv['stock_id'] == sid, 'note'] = msg
        time.sleep(0.3)
    df_csv.to_csv(CSV_PATH, encoding="utf-8-sig", index=False)
    conn.close()
    print("\n所有財報資料更新完成！")


if __name__ == "__main__":
    update_quarterly_financials()
