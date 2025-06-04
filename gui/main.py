#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
master_gui_with_analysis.py

一個簡單的 GUI 範例，包含：
1. 分頁 (Tab) 設計：
   - 第一個分頁：「資料與快速分析」 (Fetch + Quick Analysis)
   - 第二個分頁：「深入分析」  (Technical Indicator Analysis)
2. 第一分頁具備：
   - 輸入「起始日期」的欄位
   - 兩個按鈕：抓資料 / 執行快速分析
   - 左邊文字區：顯示抓資料日誌
   - 右邊文字區：顯示快速分析結果
3. 第二分頁具備：
   - 輸入「股票代號」欄位
   - 輸入「開始日期」和「結束日期」欄位
   - 四個勾選框：RSI、MACD、Bollinger、KD
   - 一個按鈕：「開始分析」
   - 下方文字區：顯示技術指標計算結果

使用方式：
    python master_gui_with_analysis.py

注意：請先確認已安裝以下套件：
    pip install PySide6 pandas

並且確認你的 SQLite 資料庫檔案放在「db/stockDB.db」，且裡面已有 twse_price 表。

"""

from src.analyzers.technical_indicator import TechnicalIndicatorAnalyzer
import sys
import os
import subprocess
import sqlite3
import threading
from datetime import datetime

import pandas as pd  # 用來做資料處理
from PySide6.QtCore import Qt, QTimer, Signal, QObject
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit,
    QTabWidget, QCheckBox, QMessageBox
)

# ------------------------------------------------------------------------------
# 1. 計算 PROJECT_ROOT：從本檔案往上一層，就是 stock_analysis 根目錄
# ------------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# ------------------------------------------------------------------------------
# 2. 要執行的外部抓資料腳本清單（絕對路徑）
# ------------------------------------------------------------------------------
SCRIPTS = [
    os.path.join(PROJECT_ROOT, 'scripts', 'download_all.py'),
    os.path.join(PROJECT_ROOT, 'scripts', 'download_credit_all.py'),
    os.path.join(PROJECT_ROOT, 'scripts', 'download_ticket_all.py'),
    os.path.join(PROJECT_ROOT, 'src', 'pipeline', 'quarterly_sql.py'),
    os.path.join(PROJECT_ROOT, 'src', 'pipeline', 'financials_sql.py'),
]

# ------------------------------------------------------------------------------
# 3. 建立必要的資料表 (IF NOT EXISTS)：只建立 twse_price / tpex_price 與法人買賣超
# ------------------------------------------------------------------------------


def ensure_price_tables(sqlite_path: str):
    """
    建立 TWSE/TPEx 股價與法人買賣超的資料表，
    如果不存在就建立。
    """
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # 1) TWSE 股價 (twse_price)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS twse_price (
        證券代號   TEXT,
        證券名稱   TEXT,
        成交股數   INTEGER,
        成交筆數   INTEGER,
        成交金額   REAL,
        開盤價     REAL,
        最高價     REAL,
        最低價     REAL,
        收盤價     REAL,
        漲跌       TEXT,
        漲跌價差   REAL,
        本益比     REAL,
        日期       TEXT,
        PRIMARY KEY (證券代號, 日期)
    );
    """)

    # 2) TWSE 法人買賣超 (twse_institutional_chip)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS twse_institutional_chip (
        證券代號       TEXT,
        證券名稱       TEXT,
        外資買賣超     INTEGER,
        投信買賣超     INTEGER,
        自營商買賣超   INTEGER,
        三大法人合計   INTEGER,
        日期           TEXT,
        PRIMARY KEY (證券代號, 日期)
    );
    """)

    # 3) TPEx 股價 (tpex_price)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tpex_price (
        證券代號   TEXT,
        證券名稱   TEXT,
        收盤價     REAL,
        最高價     REAL,
        最低價     REAL,
        成交股數   INTEGER,
        成交金額   INTEGER,
        日期       TEXT,
        PRIMARY KEY (證券代號, 日期)
    );
    """)

    # 4) TPEx 法人買賣超 (tpex_institutional_chip)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tpex_institutional_chip (
        證券代號       TEXT,
        證券名稱       TEXT,
        外資買賣超     INTEGER,
        投信買賣超     INTEGER,
        自營買賣超     INTEGER,
        三大法人合計   INTEGER,
        日期           TEXT,
        PRIMARY KEY (證券代號, 日期)
    );
    """)

    conn.commit()
    conn.close()


# ------------------------------------------------------------------------------
# 4. 自訂一個簡單的 QObject，用來放 Signal（用於從背景執行緒把 log 傳回 UI）
# ------------------------------------------------------------------------------


class FetchLogger(QObject):
    # 定義一個能傳送字符串的 signal
    log_signal = Signal(str)


# ------------------------------------------------------------------------------
# 5. 引入技術指標計算器
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 6. 主視窗類別：放置 UI 元件，並且處理「執行抓資料」與「執行分析」兩個按鈕
# ------------------------------------------------------------------------------
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("一鍵執行股市分析程式 (含分析功能)")
        self.resize(900, 600)

        # --------------------------------------------------------------
        # -- 6.1 在 __init__ 中先建立所有需要的元件（供兩個分頁使用）
        # --------------------------------------------------------------

        # 6.1.1 《Fetch Tab》 (資料與快速分析) 需要的元件
        self.label_date = QLabel("起始日期 (YYYYMMDD)：")
        self.edit_date = QLineEdit()
        self.btn_fetch = QPushButton("執行抓資料")
        self.btn_analyze = QPushButton("執行快速分析")
        self.text_fetch_log = QTextEdit()
        self.text_fetch_log.setReadOnly(True)
        self.text_analysis_log = QTextEdit()
        self.text_analysis_log.setReadOnly(True)

        # 6.1.2 《Analysis Tab》 (深入分析) 需要的元件
        self.edit_stock_code = QLineEdit()
        self.edit_start = QLineEdit()
        self.edit_end = QLineEdit()
        self.chk_rsi = QCheckBox("RSI")
        self.chk_macd = QCheckBox("MACD")
        self.chk_boll = QCheckBox("Bollinger")
        self.chk_kd = QCheckBox("KD")
        self.btn_start_analysis = QPushButton("開始分析")
        self.text_indicator_result = QTextEdit()
        self.text_indicator_result.setReadOnly(True)

        # --------------------------------------------------------------
        # -- 6.2 建立分頁容器，並把兩個分頁非同步 UI 放進去
        # --------------------------------------------------------------
        self.tab_widget = QTabWidget()

        # 6.2.1 第一個分頁：fetch_tab (資料與快速分析)
        self.fetch_tab = QWidget()
        self.init_fetch_tab_ui()
        self.tab_widget.addTab(self.fetch_tab, "資料與快速分析")

        # 6.2.2 第二個分頁：analysis_tab (深入分析)
        self.analysis_tab = QWidget()
        self.init_analysis_tab_ui()
        self.tab_widget.addTab(self.analysis_tab, "深入分析")

        # 把 tab_widget 放到 MainWindow 的主要版面裡
        main_layout = QVBoxLayout(self)
        main_layout.addWidget(self.tab_widget)

        # --------------------------------------------------------------
        # -- 6.3 建立 FetchLogger，並把 log_signal 綁到 text_fetch_log.append
        # --------------------------------------------------------------
        self.logger = FetchLogger()
        self.logger.log_signal.connect(self.text_fetch_log.append)

        # --------------------------------------------------------------
        # -- 6.4 綁定按鈕事件
        # --------------------------------------------------------------
        self.btn_fetch.clicked.connect(self.on_fetch_clicked)
        self.btn_analyze.clicked.connect(self.on_quick_analyze_clicked)
        self.btn_start_analysis.clicked.connect(self.on_start_analysis_clicked)

    # ------------------------------------------------------------------------------
    # 7. 初始化「Fetch Tab」(資料與快速分析) UI
    # ------------------------------------------------------------------------------
    def init_fetch_tab_ui(self):
        """
        把第一個分頁 (fetch_tab) 的版面，依照：
          1) 起始日期 輸入欄
          2) 按鈕：執行抓資料 / 執行快速分析
          3) 兩個文字區並排：左邊抓資料日誌、右邊快速分析結果
        排列好。
        """
        layout = QVBoxLayout(self.fetch_tab)

        # 第1排：起始日期
        h_layout_date = QHBoxLayout()
        h_layout_date.addWidget(self.label_date)
        h_layout_date.addWidget(self.edit_date)
        layout.addLayout(h_layout_date)

        # 第2排：兩個按鈕
        h_layout_buttons = QHBoxLayout()
        h_layout_buttons.addWidget(self.btn_fetch)
        h_layout_buttons.addWidget(self.btn_analyze)
        layout.addLayout(h_layout_buttons)

        # 第3排：兩個文字區並排
        h_layout_texts = QHBoxLayout()
        h_layout_texts.addWidget(self.text_fetch_log)
        h_layout_texts.addWidget(self.text_analysis_log)
        layout.addLayout(h_layout_texts)

    # ------------------------------------------------------------------------------
    # 8. 初始化「Analysis Tab」(深入分析) UI
    # ------------------------------------------------------------------------------
    def init_analysis_tab_ui(self):
        """
        把第二個分頁 (analysis_tab) 的版面，依照：
          1) 股票代號 輸入欄
          2) 開始 / 結束 日期輸入欄
          3) 技術指標勾選 (RSI, MACD, Bollinger, KD)
          4) 按鈕：開始分析
          5) 結果顯示區 (QTextEdit)
        排列好。
        """
        layout = QVBoxLayout(self.analysis_tab)

        # 第1排：股票代號
        h1 = QHBoxLayout()
        label_code = QLabel("股票代號：")
        h1.addWidget(label_code)
        h1.addWidget(self.edit_stock_code)
        layout.addLayout(h1)

        # 第2排：開始 / 結束 日期
        h2 = QHBoxLayout()
        label_start = QLabel("開始日期 (YYYYMMDD)：")
        h2.addWidget(label_start)
        h2.addWidget(self.edit_start)
        label_end = QLabel("結束日期 (YYYYMMDD)：")
        h2.addWidget(label_end)
        h2.addWidget(self.edit_end)
        layout.addLayout(h2)

        # 第3排：技術指標勾選框
        h3 = QHBoxLayout()
        h3.addWidget(self.chk_rsi)
        h3.addWidget(self.chk_macd)
        h3.addWidget(self.chk_boll)
        h3.addWidget(self.chk_kd)
        layout.addLayout(h3)

        # 第4排：開始分析按鈕
        layout.addWidget(self.btn_start_analysis)

        # 第5排：顯示技術指標計算結果
        layout.addWidget(self.text_indicator_result)

    # ------------------------------------------------------------------------------
    # 9. 按下「執行抓資料」按鈕事件：啟動背景執行緒去跑抓資料腳本
    # ------------------------------------------------------------------------------
    def on_fetch_clicked(self):
        """
        按下「執行抓資料」：把後續工作丟到背景執行緒 (thread)，
        以免堵住 UI 主線程。
        """
        # 把按鈕變灰，避免重複點
        self.btn_fetch.setEnabled(False)

        # 啟動背景執行緒
        worker = threading.Thread(target=self.fetch_all_task, daemon=True)
        worker.start()

    # ------------------------------------------------------------------------------
    # 10. 背景執行緒要做的工作 (fetch_all_task)
    # ------------------------------------------------------------------------------
    def fetch_all_task(self):
        """
        背景執行緒要做的工作：
        A) 建立資料表
        B) 檢查使用者輸入的起始日期
        C) 依序執行 SCRIPTS 裡的每一支程式
           1) emit 一行「▶️ 正在執行：XXX」
           2) 執行子程式，並逐行 emit 子程式的 stdout
        最後再把按鈕恢復可按。
        """
        # **DEBUG**：先告訴自己，背景執行緒已經進來這裡了
        self.logger.log_signal.emit("▶️ fetch_all_task() 被觸發了！")

        # A) 建立資料表
        try:
            from src.utils.config_loader import load_config
            cfg = load_config()
            sqlite_path = cfg["paths"]["sqlite"]
            ensure_price_tables(sqlite_path)
            self.logger.log_signal.emit("✅ 已確認 TWSE/TPEx 相關表格存在。\n")
        except Exception as e:
            self.logger.log_signal.emit(f"❌ 建立資料表失敗：{e}")
            # 背景執行緒結束後，把按鈕恢復
            QTimer.singleShot(0, lambda: self.btn_fetch.setEnabled(True))
            return

        # B) 檢查使用者輸入的起始日期
        start_str = self.edit_date.text().strip()
        if len(start_str) != 8 or not start_str.isdigit():
            self.logger.log_signal.emit("❌ 請輸入正確格式的起始日期，例如 20250501")
            QTimer.singleShot(0, lambda: self.btn_fetch.setEnabled(True))
            return
        try:
            datetime.strptime(start_str, "%Y%m%d")
        except ValueError:
            self.logger.log_signal.emit("❌ 日期格式錯誤，請檢查年份、月份、日期是否正確")
            QTimer.singleShot(0, lambda: self.btn_fetch.setEnabled(True))
            return

        # C) 依序執行每支腳本
        self.logger.log_signal.emit(f"▶️ 開始抓資料 (起始日期：{start_str})\n")
        python_exe = sys.executable
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        for script in SCRIPTS:
            name = os.path.basename(script)

            # ① 在每支腳本真正執行前，先 emit 一行「▶️ 正在執行：XXX」
            self.logger.log_signal.emit(f"▶️ 正在執行：{name}")

            if not os.path.isfile(script):
                self.logger.log_signal.emit(f"❌ 找不到檔案：{script}，請確認路徑正確！\n")
                break

            # 加上 '-u' 讓子程式不緩衝 (unbuffered)
            cmd = [python_exe, '-u', script, "--start", start_str]
            if script.endswith("quarterly_sql.py") or script.endswith("financials_sql.py"):
                cmd = [python_exe, '-u', script]

            # ② Popen 用 text=True、encoding="utf-8"、errors="replace"
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
                env=env,
                bufsize=1,
                text=True,
                encoding="utf-8",
                errors="replace"
            )

            # ③ 逐行讀取子程式 stdout，並 emit 到主線程
            for line in proc.stdout:
                self.logger.log_signal.emit(line.rstrip())

            proc.wait()
            if proc.returncode == 0:
                self.logger.log_signal.emit(f"✅ {name} 完成！\n")
            else:
                self.logger.log_signal.emit(f"❌ {name} 執行失敗，已停止後續。\n")
                break

        # 所有腳本跑完後，把按鈕恢復可按
        QTimer.singleShot(0, lambda: self.btn_fetch.setEnabled(True))

    # ------------------------------------------------------------------------------
    # 11. 按下「執行快速分析」按鈕事件：示範把左側日誌前面加上 [分析過]，貼到右側
    # ------------------------------------------------------------------------------
    def on_quick_analyze_clicked(self):
        """
        按下「執行快速分析」時呼叫：
        範例：把左側文字區的每一行前面加上 [分析過]，貼到右側。
        真正的快速分析邏輯可以自行改寫。
        """
        self.text_analysis_log.clear()

        if self.text_fetch_log.toPlainText().strip() == "":
            QMessageBox.information(self, "提示", "請先執行「抓資料」，才能進行快速分析！")
            return

        self.text_analysis_log.append("=== 開始快速分析資料 ===\n")
        fetch_logs = self.text_fetch_log.toPlainText().splitlines()
        for line in fetch_logs:
            if line.strip() != "":
                self.text_analysis_log.append(f"[分析過] {line}")
        self.text_analysis_log.append("\n✅ 快速分析完成！")

    # ------------------------------------------------------------------------------
    # 12. 按下「開始分析」按鈕事件：真正從 SQLite 撈資料 & 計算技術指標
    # ------------------------------------------------------------------------------
    def on_start_analysis_clicked(self):
        """
        按下「開始分析」時：
        1. 讀取使用者輸入的股票代號、開始日期、結束日期
        2. 判斷哪些指標被勾選 (RSI / MACD / Bollinger / KD)
        3. 從 SQLite 撈出該股票、該期間的收盤/最高/最低價
        4. 用 TechnicalIndicatorCalculator 計算所勾選指標
        5. 把結果顯示在 self.text_indicator_result
        """
        self.text_indicator_result.clear()

        # 1. 讀使用者輸入
        stock_code = self.edit_stock_code.text().strip()
        start_date = self.edit_start.text().strip()
        end_date = self.edit_end.text().strip()

        # 2. 驗證輸入：檢查是否填寫、格式是否正確
        if not stock_code:
            QMessageBox.warning(self, "輸入錯誤", "請輸入股票代號！")
            return
        if len(start_date) != 8 or not start_date.isdigit() or len(end_date) != 8 or not end_date.isdigit():
            QMessageBox.warning(self, "輸入錯誤", "請輸入完整且正確格式的開始/結束日期 (YYYYMMDD)。")
            return
        try:
            datetime.strptime(start_date, "%Y%m%d")
            datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            QMessageBox.warning(self, "輸入錯誤", "日期格式錯誤，請檢查是否為 YYYYMMDD。")
            return

        # 判斷是否至少勾選一個指標
        if not (self.chk_rsi.isChecked() or self.chk_macd.isChecked() or
                self.chk_boll.isChecked() or self.chk_kd.isChecked()):
            QMessageBox.warning(self, "輸入錯誤", "請至少勾選一個技術指標！")
            return

        # 3. 從 SQLite 撈該股票、該期間的收盤/最高/最低價
        try:
            db_path = "db/stockDB.db"  # SQLite 路徑
            conn = sqlite3.connect(db_path)
            query = f"""
                SELECT 日期, 收盤價 AS close, 最高價 AS high, 最低價 AS low
                FROM twse_price
                WHERE 證券代號 = '{stock_code}'
                  AND 日期 BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY 日期 ASC
            """
            df_price = pd.read_sql_query(query, conn)
            conn.close()
        except Exception as e:
            QMessageBox.critical(self, "資料庫錯誤", f"無法讀取資料：{e}")
            return

        # 檢查是否有資料
        if df_price.empty:
            QMessageBox.information(self, "查無資料", "在指定日期範圍內找不到任何股價資料。")
            return

        # 4. 把日期轉成 pandas 的 Datetime，並設成 index
        df_price['日期'] = pd.to_datetime(df_price['日期'], format='%Y%m%d')
        df_price.set_index('日期', inplace=True)

        # 5. 用 TechnicalIndicatorCalculator 計算所勾選的指標
        calc = TechnicalIndicatorCalculator(df_price)
        results = {}
        if self.chk_rsi.isChecked():
            results['RSI'] = calc.calculate_rsi(period=14)
        if self.chk_macd.isChecked():
            results['MACD'] = calc.calculate_macd()
        if self.chk_boll.isChecked():
            results['Bollinger'] = calc.calculate_bollinger(
                period=20, num_std=2)
        if self.chk_kd.isChecked():
            results['KD'] = calc.calculate_kd(k_period=9, d_period=3)

        # 6. 把結果顯示在文字區：簡單印出「最後 5 筆」
        self.text_indicator_result.append(
            f"=== {stock_code} 技術指標結果 (期間：{start_date} ~ {end_date}) ===\n"
        )
        for name, df_ind in results.items():
            self.text_indicator_result.append(f"--- {name} 指標（最後 5 筆） ---")
            self.text_indicator_result.append(str(df_ind.tail(5)))
            self.text_indicator_result.append("\n")


    # ------------------------------------------------------------------------------
    # 13. 程式進入點：建立 QApplication 並顯示 MainWindow
    # ------------------------------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
