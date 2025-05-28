import os
from dateutil.relativedelta import relativedelta
import pandas as pd
import sqlite3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from io import StringIO
from datetime import datetime
from src.utils.helpers import load_config

config = load_config()

# 從設定檔裡取出 db.path
db_path = config.get('paths', {}).get('sqlite')
if not db_path:
    raise KeyError("config.yaml 裡面必須有 db.path 設定")

# 檢查資料庫檔案是否存在
if not os.path.exists(db_path):
    raise FileNotFoundError(f"找不到資料庫檔案: {db_path}")

CSV_PATH = "data/stock_id/stock_id.csv"


def ensure_month_revenue_table(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS month_revenue (
        stock_id TEXT,
        ym TEXT,
        revenue INTEGER,
        mom REAL,
        last_year INTEGER,
        yoy REAL,
        last_update TEXT,
        PRIMARY KEY (stock_id, ym)
    )
    """)
    conn.commit()
    conn.close()


def tw_to_ad(ym):
    try:
        y, m = ym.split('/')
        if len(y) == 3:
            y = str(int(y) + 1911)
        return f"{y}/{int(m):02d}"
    except Exception:
        return ym


def fetch_moneydj_month_revenue(stock_id, years=1):
    url = f"https://concords.moneydj.com/z/zc/zch/zch_{stock_id}.djhtm"
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(2)
    try:
        table_html = driver.find_element(
            "id", "oMainTable").get_attribute("outerHTML")
        df = pd.read_html(StringIO(table_html))[0]
        col_row_idx = df.index[df.iloc[:, 0].astype(
            str).str.contains("年/月")].tolist()
        if not col_row_idx:
            print(f"{stock_id}: 無法找到「年/月」表頭，可能網頁結構變動")
            return None
        col_row = col_row_idx[0]
        df.columns = df.iloc[col_row]
        df = df.iloc[col_row+1:].reset_index(drop=True)
        df = df.dropna(axis=1, how='all')
        df = df.rename(columns={
            "年/月": "ym", "營收": "revenue", "月增率": "mom",
            "去年同期": "last_year", "年增率": "yoy"
        })
        df["stock_id"] = stock_id
        if years is not None and years > 0:
            df = df.head(years * 12)
        for col in ["revenue", "last_year", "cum_revenue"]:
            if col in df.columns and df[col].ndim == 1:
                try:
                    df[col] = df[col].astype(str).str.replace(
                        ",", "").replace("", "0").astype(int)
                except Exception as ex:
                    print(f"{stock_id}: 欄位 {col} 轉型失敗：{ex}")
        for col in ["mom", "yoy"]:
            if col in df.columns and df[col].ndim == 1:
                try:
                    df[col] = df[col].astype(str).str.replace("%", "").str.replace(
                        "−", "-").replace("", "0").astype(float)
                except Exception as ex:
                    print(f"{stock_id}: 欄位 {col} 轉型失敗：{ex}")
        if "年增率" in df.columns and "cum_yoy" not in df.columns:
            try:
                df["cum_yoy"] = df["年增率"].astype(str).str.replace(
                    "%", "").replace("", "0").astype(float)
            except Exception as ex:
                print(f"{stock_id}: 欄位 cum_yoy 轉型失敗：{ex}")
            df = df.drop("年增率", axis=1)
        if "cum_yoy" not in df.columns:
            df["cum_yoy"] = None
        df["last_update"] = datetime.now().strftime("%Y%m%d")
        df = df[["stock_id", "ym", "revenue", "mom", "last_year",
                 "yoy", "last_update"]]
        df["ym"] = df["ym"].apply(tw_to_ad)
        return df
    except Exception as e:
        print(f"{stock_id}: 解析失敗，錯誤：{e}")
        return None
    finally:
        driver.quit()


def get_recent_ym_list(years=2):
    now = datetime.now()
    last_month = now - relativedelta(months=1)
    ym_set = set()
    for i in range(years * 12):
        dt = last_month - relativedelta(months=i)
        ym = f"{dt.year}/{dt.month:02d}"
        ym_set.add(ym)
    return ym_set


def update_month_revenue_daily(years=2):
    ensure_month_revenue_table(db_path)
    try:
        df_csv = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df_csv = pd.read_csv(CSV_PATH, encoding="cp950")
    if 'note' not in df_csv.columns:
        df_csv['note'] = ''
    df_csv['note'] = df_csv['note'].astype(str)
    stock_ids = df_csv['stock_id'].astype(str).tolist()

    conn = sqlite3.connect(db_path)
    target_ym_set = get_recent_ym_list(years)
    for idx, sid in enumerate(stock_ids):
        print(f"\n== {sid} ==")
        cursor = conn.execute(
            "SELECT ym FROM month_revenue WHERE stock_id=?", (sid,))
        existing_ym = set()
        for row in cursor.fetchall():
            ym = row[0]
            ym = tw_to_ad(ym)
            existing_ym.add(ym)
        # print(f"現有: {existing_ym}")
        # print(f"應有: {target_ym_set}")

        info_msg = ""

        if target_ym_set <= existing_ym:
            info_msg = '---月報SQL已有完整資料---'
            print(f"{sid}: {info_msg}")
            df_csv.loc[df_csv['stock_id'].astype(
                str) == str(sid), 'note'] = info_msg
            df_csv.to_csv(CSV_PATH, encoding="utf-8-sig", index=False)
            continue

        print(">>> 會進行爬蟲")
        df = fetch_moneydj_month_revenue(sid, years=years)
        if df is None:
            info_msg = '------無法讀取月報'
            print(f"{sid}: {info_msg}")
            df_csv.loc[df_csv['stock_id'].astype(
                str) == str(sid), 'note'] = info_msg
            df_csv.to_csv(CSV_PATH, encoding="utf-8-sig", index=False)
            continue

        new_df = df[~df["ym"].isin(existing_ym)].copy()
        if new_df.empty:
            info_msg = '目標月份皆已存在'
            print(f"{sid}: {info_msg}")
            df_csv.loc[df_csv['stock_id'].astype(
                str) == str(sid), 'note'] = info_msg
            df_csv.to_csv(CSV_PATH, encoding="utf-8-sig", index=False)
            continue

        info_msg = f"成功寫入{len(new_df)}筆資料"
        print(f"{sid}: {info_msg}")
        print(new_df[["ym", "revenue"]].to_string(index=False))
        new_df.to_sql("month_revenue", conn, if_exists="append", index=False)
        df_csv.loc[df_csv['stock_id'].astype(
            str) == str(sid), 'note'] = info_msg
        df_csv.to_csv(CSV_PATH, encoding="utf-8-sig", index=False)
        time.sleep(1)
    conn.close()
    print("\n全部執行結束！")


if __name__ == "__main__":
    update_month_revenue_daily(years=2)
