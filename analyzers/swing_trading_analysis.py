import sqlite3
import pandas as pd
import os

# 使用動態路徑取得資料庫位置（回到上層目錄 db 資料夾）
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', 'db', 'stockDB.db')


def analyze_swing_stock(
    stock_id: str,
    ma_short_window: int = 20,
    ma_long_window: int = 60,
    macd_short_span: int = 12,
    macd_long_span: int = 26,
    macd_signal_span: int = 9,
    rsi_window: int = 14,
    atr_window: int = 14,
    bb_window: int = 20,
    bb_std_factor: float = 2.0
) -> pd.DataFrame:
    """
    波段交易模型全量分析，回傳包含技術指標、訊號與評分的 DataFrame
    指標參數化後可在呼叫時調整：
      - ma_short_window / ma_long_window：短/長均線天數
      - macd_short_span / macd_long_span / macd_signal_span：MACD 快慢訊號線
      - rsi_window：RSI 計算週期
      - atr_window：ATR 計算週期
      - bb_window / bb_std_factor：布林帶週期與倍數
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        # 取得必要欄位：高、低、收盤
        query = (
            "SELECT 日期, 證券代號, 證券名稱, 最高價, 最低價, 收盤價 "
            "FROM twse_chip "
            "WHERE 證券代號 = ? "
            "ORDER BY 日期"
        )
        df = pd.read_sql_query(query, conn, params=(stock_id,))
        df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
        for col in ['最高價', '最低價', '收盤價']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 1. 移動平均線
        df['MA_short'] = df['收盤價'].rolling(window=ma_short_window, min_periods=1).mean()
        df['MA_long']  = df['收盤價'].rolling(window=ma_long_window, min_periods=1).mean()

        # 2. MACD
        ema_short = df['收盤價'].ewm(span=macd_short_span, adjust=False).mean()
        ema_long  = df['收盤價'].ewm(span=macd_long_span, adjust=False).mean()
        df['MACD']        = ema_short - ema_long
        df['MACD_SIGNAL'] = df['MACD'].ewm(span=macd_signal_span, adjust=False).mean()
        df['MACD_HIST']   = df['MACD'] - df['MACD_SIGNAL']

        # 3. RSI
        delta = df['收盤價'].diff()
        gain  = delta.clip(lower=0)
        loss  = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=rsi_window, min_periods=1).mean()
        avg_loss = loss.rolling(window=rsi_window, min_periods=1).mean()
        rs = avg_gain / avg_loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # 4. True Range & ATR
        df['prev_close'] = df['收盤價'].shift(1)
        df['H-L']  = df['最高價'] - df['最低價']
        df['H-PC'] = (df['最高價'] - df['prev_close']).abs()
        df['L-PC'] = (df['最低價'] - df['prev_close']).abs()
        df['TR']   = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
        df['ATR']  = df['TR'].rolling(window=atr_window, min_periods=1).mean()

        # 5. 布林帶
        df['BB_MID'] = df['收盤價'].rolling(window=bb_window, min_periods=1).mean()
        df['BB_STD'] = df['收盤價'].rolling(window=bb_window, min_periods=1).std()
        df['BB_UP']  = df['BB_MID'] + bb_std_factor * df['BB_STD']
        df['BB_DN']  = df['BB_MID'] - bb_std_factor * df['BB_STD']

        # 篩除 NaN
        df_valid = df.dropna(subset=[
            'MA_short', 'MA_long', 'MACD', 'RSI', 'ATR', 'BB_UP', 'BB_DN'
        ])
        if df_valid.empty:
            return pd.DataFrame([{
                '股票代號': stock_id,
                '股票名稱': '無足夠資料',
                '日期': None,
                '收盤價': None,
                'MA_short': None,
                'MA_long': None,
                'MACD': None,
                'RSI': None,
                'ATR': None,
                'BB_UP': None,
                'BB_DN': None,
                '技術訊號': '資料不足',
                '評分': 0
            }])

        latest = df_valid.iloc[-1]
        prev   = df_valid.iloc[-2] if df_valid.shape[0] >= 2 else latest

        signals = []
        score = 50

        # 傳統指標評分
        if latest['MA_short'] > latest['MA_long']:
            signals.append('多頭排列');            score += 25
        if prev['MA_short'] < prev['MA_long'] and latest['MA_short'] > latest['MA_long']:
            signals.append('黃金交叉');            score += 20
        if latest['MACD_HIST'] > 0:
            signals.append('MACD 正向');           score += 15
        if prev['MACD'] < prev['MACD_SIGNAL'] and latest['MACD'] > latest['MACD_SIGNAL']:
            signals.append('MACD 黃金交叉');        score += 15
        if latest['RSI'] < 30:
            signals.append('RSI 超賣');            score += 10
        elif latest['RSI'] > 70:
            signals.append('RSI 超買');            score -= 10

        # 波動性評分整合
        atr_mean = df_valid['ATR'].mean()
        if latest['ATR'] > atr_mean:
            signals.append('高波動(ATR)');        score -= 10

        if latest['收盤價'] > latest['BB_UP']:
            signals.append('布林帶上軌突破');      score += 10
        elif latest['收盤價'] < latest['BB_DN']:
            signals.append('布林帶下軌跌破');      score += 10

        score = max(0, min(100, score))

        return pd.DataFrame([{
            '股票代號': stock_id,
            '股票名稱': latest['證券名稱'],
            '日期': latest['日期'].date(),
            '收盤價': latest['收盤價'],
            'MA_short': latest['MA_short'],
            'MA_long': latest['MA_long'],
            'MACD': latest['MACD'],
            'RSI': latest['RSI'],
            'ATR': latest['ATR'],
            'BB_UP': latest['BB_UP'],
            'BB_DN': latest['BB_DN'],
            '技術訊號': ', '.join(signals) if signals else '無明顯訊號',
            '評分': score
        }])
    finally:
        conn.close()


def get_swing_score(
    stock_id: str,
    **indicator_params
) -> int:
    """
    取得波段交易模型的分數，支援傳入指標參數覆寫預設值
    例如：get_swing_score('2330', ma_short_window=10, rsi_window=7)
    """
    df = analyze_swing_stock(stock_id, **indicator_params)
    if df.empty or '評分' not in df.columns:
        return 0
    return int(df.at[0, '評分'])
