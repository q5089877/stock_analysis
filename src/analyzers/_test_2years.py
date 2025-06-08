import os
import sqlite3
import pandas as pd
from technical_indicator import TechnicalIndicatorAnalyzer
from financial_analyzer import FinancialAnalyzer

# ─── 全域參數（固定不動）────────────────────────────────
DB_PATH = 'db/stockDB.db'
STOCK_POOL_SIZE = 100
BACKTEST_START_DATE = '2023-06-08'
BACKTEST_END_DATE = '2025-06-07'
INITIAL_CAPITAL_PER_STK = 20000
FEE_RATE = 0.003
MAX_HOLD_DAYS = 120

# 財務門檻固定
FIN_ENTRY_THRESHOLD = 70
FIN_EXIT_THRESHOLD = 50

# ─── 讀取所有產業別 ───────────────────────────────────────


def load_industries(csv_path='data/stock_id/stock_id.csv'):
    df = pd.read_csv(csv_path, dtype=str)
    return df['產業別'].dropna().unique().tolist()

# ─── 載入指定產業的樣本股票清單 ─────────────────────────────


def load_sample_tickers(industry, csv_path='data/stock_id/stock_id.csv'):
    conn = sqlite3.connect(DB_PATH)
    try:
        ind_df = pd.read_sql(
            "SELECT DISTINCT stock_id FROM stock_info WHERE industry = ?", conn,
            params=(industry,)
        )
        ind_list = ind_df['stock_id'].astype(str).tolist()
    except Exception:
        csv_df = pd.read_csv(csv_path, dtype=str)
        ind_list = csv_df[csv_df['產業別'] == industry]['stock_id'].tolist()

    eps_df = pd.read_sql(
        "SELECT DISTINCT stock_id FROM quarterly_income_statement WHERE eps != 0", conn
    )
    eps_list = eps_df['stock_id'].astype(str).tolist()
    conn.close()

    # 交集並取前 N 檔
    eligible = [t for t in ind_list if t in eps_list]
    return eligible[:STOCK_POOL_SIZE]

# ─── 執行一次回測並回傳平均報酬率 ───────────────────────────


def run_backtest(tech_entry, tech_exit, industry):
    tech = TechnicalIndicatorAnalyzer(DB_PATH, market="twse")
    fin = FinancialAnalyzer(DB_PATH)
    sample_tickers = load_sample_tickers(industry)

    results = []
    for ticker in sample_tickers:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"""
            SELECT 日期 AS date, 開盤價 AS open, 收盤價 AS close,
                   最高價 AS high, 最低價 AS low
            FROM twse_price
            WHERE 證券代號 = '{ticker}'
              AND 日期 BETWEEN '{BACKTEST_START_DATE}' AND '{BACKTEST_END_DATE}'
            ORDER BY date
        """, conn)
        conn.close()
        if df.empty:
            continue

        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df.set_index('date', inplace=True)

        # 技術面分數
        rsi = tech._calculate_rsi(df['close'], 14)
        macd = tech._calculate_macd_diff(df['close'], 12, 26, 9)
        kd_df = tech._calculate_kd(df['close'], df['high'], df['low'], 9, 3, 3)
        ind = pd.DataFrame({'RSI': rsi, 'MACD_diff': macd,
                            'KD_K': kd_df['K'], 'KD_D': kd_df['D']})

        def map_score(v, low, high):
            if pd.isna(v):
                return 0.0
            if v <= low:
                return 100.0
            if v >= high:
                return 0.0
            return (high - v) / (high - low) * 100

        df['tech_score'] = ind.apply(lambda r: (
            map_score(r['RSI'], 30, 70)
            + ((max(min(r['MACD_diff'], 2), -2) + 2) / 4 * 100)
            + map_score(r['KD_K'], 20, 80)
            + map_score(r['KD_D'], 20, 80)
        ) / 4, axis=1)

        # 財務面分數
        fscore = fin.get_financial_score(ticker)

        # 交易信號
        df['entry'] = (df['tech_score'] >= tech_entry) & (
            (fscore <= 0) | (fscore >= FIN_ENTRY_THRESHOLD)
        )
        df['exit'] = (df['tech_score'] <= tech_exit) | (
            (fscore > 0) & (fscore <= FIN_EXIT_THRESHOLD)
        )

        # 回測模擬
        cash, pos = INITIAL_CAPITAL_PER_STK, 0.0
        buy_dt = None
        dates = df.index.to_list()
        for i in range(1, len(dates)):
            prev, today = dates[i-1], dates[i]
            price = df.at[today, 'open']
            if pd.isna(price) or price <= 0:
                continue
            if pos == 0 and df.at[prev, 'entry']:
                pos = cash * (1 - FEE_RATE) / price
                cash = 0
                buy_dt = today
            elif pos > 0 and (df.at[prev, 'exit'] or (today - buy_dt).days >= MAX_HOLD_DAYS):
                cash = pos * price * (1 - FEE_RATE)
                pos = 0
        if pos > 0:
            last_price = df['open'].iloc[-1]
            cash = pos * last_price * (1 - FEE_RATE)

        profit = (cash - INITIAL_CAPITAL_PER_STK) / \
            INITIAL_CAPITAL_PER_STK * 100
        results.append(profit)

    return round(sum(results) / len(results), 2) if results else 0.0


# ─── 主程式：各產業參數網格搜尋並輸出 ─────────────────────
if __name__ == '__main__':
    industries = load_industries()
    summary = []

    for industry in industries:
        best = {'entry': None, 'exit': None, 'win_rate': -999}
        run_count = 0
        total_count = sum(
            len(range(20, e, 5)) for e in range(40, 81, 5)
        )
        print(f"開始處理產業：{industry}，共 {total_count} 種參數組合")

        for tech_entry in range(40, 81, 5):
            for tech_exit in range(20, tech_entry, 5):
                run_count += 1
                win_rate = run_backtest(tech_entry, tech_exit, industry)
                print(
                    f"[{run_count}/{total_count}] {industry} - 進場={tech_entry}, 出場={tech_exit} → {win_rate}%")
                if win_rate > best['win_rate']:
                    best.update(
                        {'entry': tech_entry, 'exit': tech_exit, 'win_rate': win_rate})

        summary.append({
            '產業別': industry,
            '最佳進場門檻': best['entry'],
            '最佳出場門檻': best['exit'],
            '最高平均報酬率(%)': best['win_rate']
        })
        print(
            f"→ {industry} 最佳：Entry={best['entry']} / Exit={best['exit']}，平均報酬率={best['win_rate']}%\n")

    # 輸出 CSV
    out_df = pd.DataFrame(summary)
    os.makedirs('output', exist_ok=True)
    out_csv = 'output/two_years_analyzers.csv'
    out_df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f"所有產業處理完畢，結果輸出至 {out_csv}")
