import sqlite3
from datetime import datetime
import requests
import pandas as pd


class MOPSFinancialDownloader:
    """
    下載公開資訊觀測站（MOPS）季報資料，包括損益表與資產負債表
    """

    def download(self, report_date: str) -> dict:
        # report_date: 'YYYY-MM-DD'
        dt = datetime.strptime(report_date, '%Y-%m-%d').date()
        roc_year = dt.year - 1911
        season_map = {3: '01', 6: '02', 9: '03', 12: '04'}
        season = season_map.get(dt.month)
        if not season:
            print(f"⚠️ 無效的季報月份: {dt.month}")
            return {}

        url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_2'
        payload = {
            'encodeURIComponent': '1',
            'step': '1',
            'firstin': '1',
            'off': '1',
            'year': str(roc_year),
            'season': season,
        }
        try:
            resp = requests.post(url, data=payload, timeout=30)
            resp.encoding = 'big5'
            tables = pd.read_html(resp.text)
        except Exception as e:
            print(f"❌ MOPS 下載或解析失敗: {e}")
            return {}

        try:
            # 假設 tables[0] 為損益表，tables[2] 為資產負債表
            income_raw = tables[0]
            balance_raw = tables[2]
            # 設定索引與欄位
            income_df = income_raw.set_index(income_raw.columns[0]).iloc[:, 1:]
            balance_df = balance_raw.set_index(
                balance_raw.columns[0]).iloc[:, 1:]
            # 轉置為：columns=stock_id, index=科目
            income_df = income_df.T
            balance_df = balance_df.T
            return {'income_statement': income_df, 'balance_sheet': balance_df}
        except Exception as e:
            print(f"⚠️ MOPS 表格結構異常，解析失敗: {e}")
            return {}


class FundamentalCalculator:
    """
    計算基本面指標：ROE、營業毛利率
    """

    def compute(self, data: dict, report_date: str) -> list:
        income_df = data.get('income_statement')
        balance_df = data.get('balance_sheet')
        records = []

        if income_df is None or balance_df is None:
            print(f"⚠️ 資料不足，無法計算 {report_date} 的指標。")
            return records

        for stock_id in income_df.index:
            try:
                row_income = income_df.loc[stock_id]
                row_balance = balance_df.loc[stock_id]
                net_income = float(row_income.get(
                    '本期稅後淨利', row_income.get('本期淨利', pd.NA)))
                revenue = float(row_income.get('營業收入', pd.NA))
                gross_profit = float(row_income.get(
                    '營業毛利', revenue - float(row_income.get('營業成本', 0))))
                equity = float(row_balance.get('股東權益總計', pd.NA))

                roe = net_income / equity if equity and equity != 0 else None
                gross_margin = gross_profit / revenue if revenue and revenue != 0 else None

                records.append({
                    'stock_id': stock_id,
                    'date': report_date,
                    'roe': roe,
                    'gross_margin': gross_margin
                })
            except Exception as e:
                print(f"⚠️ 跳過 {stock_id} @ {report_date}: {e}")
        return records


class FinanceDBWriter:
    """
    將計算後的財務指標寫入 SQLite 資料表
    """

    def __init__(self, conn: sqlite3.Connection, table_name: str):
        self.conn = conn
        self.table_name = table_name

    def ensure_table(self):
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            stock_id TEXT,
            date TEXT,
            roe REAL,
            gross_margin REAL,
            PRIMARY KEY (stock_id, date)
        );
        '''
        self.conn.execute(create_sql)
        self.conn.commit()

    def write(self, records: list):
        if not records:
            print(f"ℹ️ 無資料寫入 {self.table_name}")
            return
        cursor = self.conn.cursor()
        for rec in records:
            cursor.execute(
                f"INSERT OR REPLACE INTO {self.table_name} "
                "(stock_id, date, roe, gross_margin) VALUES (?, ?, ?, ?)",
                (rec['stock_id'], rec['date'], rec['roe'], rec['gross_margin'])
            )
        self.conn.commit()
        print(f"✅ 已寫入 {len(records)} 筆至 {self.table_name}")


def import_finance_indicators(config: dict):
    """
    下載並寫入 ROE & 毛利率至 SQLite
    """
    sqlite_path = config['paths']['sqlite']
    table_name = config['finance'].get('table_name', 'finance_statements')
    report_dates = config['finance'].get('report_dates', [])

    conn = sqlite3.connect(sqlite_path)
    writer = FinanceDBWriter(conn, table_name)
    writer.ensure_table()

    downloader = MOPSFinancialDownloader()
    calculator = FundamentalCalculator()

    for rd in report_dates:
        print(f"🔄 開始處理 {rd} 的財務指標...")
        data = downloader.download(rd)
        records = calculator.compute(data, rd)
        writer.write(records)

    conn.close()
    print("🎉 財務指標匯入完成！")
