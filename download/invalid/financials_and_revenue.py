# financials_and_revenue_no_etf_api.py

import os
import time
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from FinMind.data import DataLoader
from typing import TypeVar, Callable, List

# -------- 1. 參數設定 --------
TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wNS0xNyAyMDoyNToxMSIsInVzZXJfaWQiOiJuZWlsNDIyIiwiaXAiOiIxMjMuMTkyLjE3OS4xNzEifQ.HuZapURslwVx-CX2AOAhQwY2ufzTDA9AYqeRkVpXmrA'
API_URL = 'https://api.finmindtrade.com/api/v4/data'
DB_PATH = './db/financials.db'
RATE_LIMIT = 600
INTERVAL = 3600.0 / RATE_LIMIT
YEARS = 1.0
# --------------------------------

T = TypeVar('T')


def rate_limited(func: Callable[..., T], *args, **kwargs) -> T:
    result: T = func(*args, **kwargs)
    time.sleep(INTERVAL)
    return result


def retry(func: Callable[..., T], *args, retries: int = 3, backoff: int = 2, **kwargs) -> T:
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(backoff ** i)
    raise RuntimeError('retry did not return a value')


def load_company_stock_ids(token: str) -> List[str]:
    """
    1) 呼叫 API 拿 TaiwanStockInfo
    2) 用 industry_category 過濾掉含 ETF 的
    3) 回傳剩餘的 stock_id
    """
    resp = requests.get(API_URL, params={
        "dataset": "TaiwanStockInfo",
        "token": token
    })
    data = resp.json().get("data", [])
    df = pd.DataFrame(data)
    total = len(df)

    # 過濾：industry_category 含 'ETF'
    mask = (
        ~df['industry_category'].str.contains('ETF', na=False)
        & ~df['industry_category'].str.contains('ETN', na=False)
        & ~df['industry_category'].str.contains('Index', na=False)
        & ~df['industry_category'].str.contains('證券', na=False)
    )
    filtered = df[mask]
    kept = len(filtered)

    print(f"[INFO] 從 API 拿到 {total} 檔股票，排除 ETF 後保留 {kept} 檔")
    return filtered['stock_id'].astype(str).tolist()


def gen_quarter_ends(start_date: datetime, end_date: datetime) -> List[str]:
    quarters: List[str] = []
    dt = start_date.replace(day=1)
    while dt < end_date:
        m = ((dt.month - 1) // 3 + 1) * 3
        q_end = dt.replace(month=m, day=1) + \
            relativedelta(months=1) - timedelta(days=1)
        if start_date <= q_end <= end_date:
            quarters.append(q_end.strftime('%Y-%m-%d'))
        dt += relativedelta(months=3)
    return quarters


# -------- 2. 初始化 API & DB --------
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
api = DataLoader()
api.login_by_token(api_token=TOKEN)

conn = sqlite3.connect(DB_PATH)
conn.execute("""
CREATE TABLE IF NOT EXISTS financial_statements (
    stock_id       TEXT NOT NULL,
    report_date    TEXT NOT NULL,
    statement_type TEXT NOT NULL,
    item_name      TEXT NOT NULL,
    value          REAL NOT NULL,
    origin_name    TEXT,
    PRIMARY KEY (stock_id, report_date, statement_type, item_name)
);
""")
conn.execute("""
CREATE TABLE IF NOT EXISTS sync_progress (
    id INTEGER PRIMARY KEY CHECK (id=1),
    last_idx INTEGER NOT NULL
);
""")
if conn.execute("SELECT COUNT(*) FROM sync_progress").fetchone()[0] == 0:
    conn.execute("INSERT INTO sync_progress(id, last_idx) VALUES (1, 0)")
conn.commit()
last_idx = conn.execute(
    "SELECT last_idx FROM sync_progress WHERE id=1").fetchone()[0]

# -------- 3. 計算日期區間 --------
today = datetime.today()
start_date = today - relativedelta(years=YEARS)
quarter_ends = gen_quarter_ends(start_date, today)
start_str = start_date.strftime('%Y-%m-%d')
end_str = today.strftime('%Y-%m-%d')

# -------- 4. 載入股票清單（排除 ETF） --------
stock_ids = load_company_stock_ids(TOKEN)
print(f"從 idx {last_idx+1} 開始下載，共 {len(stock_ids)} 檔股票")

# -------- 5. 主流程：抓月營收 + 季報 --------
for idx, stock in enumerate(stock_ids, 1):
    if idx <= last_idx:
        continue
    print(f"[{idx}/{len(stock_ids)}] 處理 {stock} ...")

    # 月營收
    try:
        df_rev = retry(rate_limited,
                       api.taiwan_stock_month_revenue,
                       stock_id=stock,
                       start_date=start_str,
                       end_date=end_str)
        if df_rev.empty:
            print(f"[INFO] 月營收無資料：{stock}")
        else:
            for _, r in df_rev.iterrows():
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO financial_statements VALUES (?,?,?,?,?,?)",
                        (stock, r['date'], 'monthly_revenue',
                         'monthly_revenue', float(r['revenue']), 'revenue')
                    )
                except Exception as db_e:
                    print(f"[ERROR] 月營收寫入失敗: {stock}@{r['date']} - {db_e}")
            print(f"[INFO] 月營收寫入成功：{stock} 共 {len(df_rev)} 筆")
    except Exception as e:
        print(f"[ERROR] 月營收下載失敗 {stock}: {e}")

    # 季報三表
    for q in quarter_ends:
        for stmt, fn in (
            ('income',     api.taiwan_stock_financial_statement),
            ('balance',    api.taiwan_stock_balance_sheet),
            ('cash_flow',  api.taiwan_stock_cash_flows_statement),
        ):
            try:
                df_fs = retry(rate_limited, fn, stock_id=stock, start_date=q)
                if df_fs.empty:
                    print(f"[INFO] {stmt} 無資料：{stock}@{q}")
                else:
                    for _, r in df_fs.iterrows():
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO financial_statements VALUES (?,?,?,?,?,?)",
                                (stock, r['date'], stmt, r['type'], float(
                                    r['value']), r.get('origin_name', ''))
                            )
                        except Exception as db_e:
                            print(
                                f"[ERROR] {stmt} 寫入失敗: {stock}@{r['date']} - {db_e}")
                    print(f"[INFO] {stmt} 寫入成功：{stock}@{q} 共 {len(df_fs)} 筆")
            except Exception as e:
                print(f"[ERROR] {stmt} {stock}@{q} 下載失敗: {e}")

    conn.commit()
    conn.execute("UPDATE sync_progress SET last_idx=? WHERE id=1", (idx,))
    conn.commit()

conn.close()
print("✅ 全部資料抓取並寫入完成！")
