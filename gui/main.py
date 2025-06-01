#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
master_gui_with_analysis.py

一個簡單的 GUI 範例，包含：
1. 輸入「起始日期」的欄位
2. 兩個按鈕：抓資料 / 執行分析
3. 左邊一個文字區：顯示抓資料時的訊息
4. 右邊一個文字區：顯示分析結果

執行抓資料之前，會自動「建立必要的資料表」，
並且將子行程的工作目錄設為專案根目錄 (PROJECT_ROOT)，
避免各腳本因為「找不到 src.pipeline.X」而無法匯入。
"""

import sys
import os
import subprocess
import sqlite3
from datetime import datetime

from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QMessageBox
)

# ------------------------------------------------------------------------------
# 1. 計算 PROJECT_ROOT：從本檔案往上一層，就是 stock_analysis 根目錄
# ------------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# ------------------------------------------------------------------------------
# 2. 要執行的腳本清單（絕對路徑）
# ------------------------------------------------------------------------------
SCRIPTS = [
    os.path.join(PROJECT_ROOT, 'scripts', 'download_all.py'),
    os.path.join(PROJECT_ROOT, 'scripts', 'download_credit_all.py'),
    os.path.join(PROJECT_ROOT, 'scripts', 'download_ticket_all.py'),
    os.path.join(PROJECT_ROOT, 'src', 'pipeline', 'quarterly_sql.py'),
    os.path.join(PROJECT_ROOT, 'src', 'pipeline', 'financials_sql.py'),
]

# ------------------------------------------------------------------------------
# 3. 建立必要的資料表 (IF NOT EXISTS)
# ------------------------------------------------------------------------------


def ensure_price_tables(sqlite_path: str):
    """
    建立 TWSE/TPEx 股價與法人買賣超的資料表，
    SQL 內容直接參考各 import_*.py 中的 CREATE TABLE 定義。
    """

    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # 1) TWSE 股價 (twse_chip)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS twse_chip (
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

    # 3) TPEx 股價 (tpex_chip)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tpex_chip (
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
# 4. 主視窗
# ------------------------------------------------------------------------------


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("一鍵執行股市分析程式 (含分析功能)")
        self.resize(800, 600)

        # 標籤 + 輸入框 (起始日期)
        self.label_date = QLabel("起始日期 (YYYYMMDD)：")
        self.edit_date = QLineEdit()

        # 按鈕：抓資料 / 執行分析
        self.btn_fetch = QPushButton("執行抓資料")
        self.btn_analyze = QPushButton("執行分析")

        # 文字區：左-抓資料日誌 / 右-分析結果
        self.text_fetch_log = QTextEdit()
        self.text_fetch_log.setReadOnly(True)
        self.text_analysis_log = QTextEdit()
        self.text_analysis_log.setReadOnly(True)

        # 佈局
        self.init_ui()

        # 按鈕事件
        self.btn_fetch.clicked.connect(self.on_fetch_clicked)
        self.btn_analyze.clicked.connect(self.on_analyze_clicked)

    def init_ui(self):
        """安排各個元件的佈局"""
        main_layout = QVBoxLayout(self)

        # 第1排：起始日期 標籤 + 輸入框
        h_layout_date = QHBoxLayout()
        h_layout_date.addWidget(self.label_date)
        h_layout_date.addWidget(self.edit_date)
        main_layout.addLayout(h_layout_date)

        # 第2排：兩個按鈕
        h_layout_buttons = QHBoxLayout()
        h_layout_buttons.addWidget(self.btn_fetch)
        h_layout_buttons.addWidget(self.btn_analyze)
        main_layout.addLayout(h_layout_buttons)

        # 第3排：兩個文字區 並排
        h_layout_texts = QHBoxLayout()
        h_layout_texts.addWidget(self.text_fetch_log)
        h_layout_texts.addWidget(self.text_analysis_log)
        main_layout.addLayout(h_layout_texts)

    def on_fetch_clicked(self):
        """
        按下「執行抓資料」：
        1) 建立 TWSE/TPEx 相關資料表
        2) 檢查輸入的起始日期
        3) 執行 SCRIPTS 裡的每一支程式，並把輸出顯示到左邊文字區
        """

        # 按鈕變灰，避免重複點
        self.btn_fetch.setEnabled(False)

        # 1) 建立資料表
        try:
            from src.utils.config_loader import load_config
            cfg = load_config()
            sqlite_path = cfg["paths"]["sqlite"]
            ensure_price_tables(sqlite_path)
            self.text_fetch_log.append("✅ 已確認 TWSE/TPEx 相關表格存在。\n")
        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"建立資料表失敗：{e}")
            self.btn_fetch.setEnabled(True)
            return

        # 2) 檢查使用者輸入的日期
        start_str = self.edit_date.text().strip()
        if len(start_str) != 8 or not start_str.isdigit():
            QMessageBox.warning(self, "錯誤", "請輸入正確格式的起始日期，例如 20250501")
            self.btn_fetch.setEnabled(True)
            return
        try:
            datetime.strptime(start_str, "%Y%m%d")
        except ValueError:
            QMessageBox.warning(self, "錯誤", "日期格式錯誤，請檢查年份、月份、日期是否正確")
            self.btn_fetch.setEnabled(True)
            return

        # 3) 執行每個腳本
        self.text_fetch_log.append(f"▶️ 開始抓資料 (起始日期：{start_str})\n")
        python_exe = sys.executable
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        for script in SCRIPTS:
            self.text_fetch_log.append(f"▶️ 執行 {script}")

            if not os.path.isfile(script):
                self.text_fetch_log.append(f"❌ 找不到檔案：{script}，請確認路徑正確！\n")
                break

            # 子行程執行時，指定 cwd=PROJECT_ROOT
            cmd = [python_exe, script, "--start", start_str]
            if script.endswith("quarterly_sql.py") or script.endswith("financials_sql.py"):
                cmd = [python_exe, script]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
                env=env
            )

            for byte_line in proc.stdout:
                try:
                    line = byte_line.decode('utf-8')
                except UnicodeDecodeError:
                    line = byte_line.decode('utf-8', errors='replace')
                self.text_fetch_log.append(line.rstrip())
                QApplication.processEvents()

            proc.wait()
            if proc.returncode == 0:
                self.text_fetch_log.append(
                    f"✅ {os.path.basename(script)} 完成！\n")
            else:
                self.text_fetch_log.append(
                    f"❌ {os.path.basename(script)} 執行失敗，已停止後續。\n")
                break

        self.btn_fetch.setEnabled(True)

    def on_analyze_clicked(self):
        """
        按下「執行分析」：
        示範把左邊文字區的每一行前面加上 [分析過]，顯示到右邊
        你可以在這裡自行插入讀資料庫並分析的邏輯
        """
        self.text_analysis_log.clear()

        if self.text_fetch_log.toPlainText().strip() == "":
            QMessageBox.information(self, "提示", "請先執行「抓資料」，才能進行分析！")
            return

        self.text_analysis_log.append("=== 開始分析資料 ===\n")
        fetch_logs = self.text_fetch_log.toPlainText().splitlines()
        for line in fetch_logs:
            if line.strip() != "":
                self.text_analysis_log.append(f"[分析過] {line}")
        self.text_analysis_log.append("\n✅ 分析完成！這裡可以顯示統計結果、建議等。")


# ------------------------------------------------------------------------------
# 5. 程式進入點：建立 QApplication 並顯示 MainWindow
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
