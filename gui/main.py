#!/usr/bin/env python3
import sys
import os
import subprocess
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QTextEdit

# 專案根目錄
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# 要執行的腳本路徑清單
SCRIPTS = [
    os.path.join(PROJECT_ROOT, 'scripts', 'download_all.py'),
    os.path.join(PROJECT_ROOT, 'src', 'pipeline', 'quarterly_sql.py'),
    os.path.join(PROJECT_ROOT, 'src', 'pipeline', 'financials_sql.py'),
]


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("一鍵執行股市分析腳本 (Qt6)")
        self.resize(600, 400)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        self.btn_run = QPushButton("執行全部程式")
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        layout.addWidget(self.btn_run)
        layout.addWidget(self.log)
        self.btn_run.clicked.connect(self.run_all)

    def run_all(self):
        self.btn_run.setEnabled(False)
        # 強制子行程輸出 UTF-8
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        for script in SCRIPTS:
            self.log.append(f"▶️ 執行 {script}")
            if not os.path.isfile(script):
                self.log.append(f"❌ 檔案不存在：{script}，請確認路徑！")
                break

            # 以二進位方式讀取 stdout 檔管
            proc = subprocess.Popen(
                [sys.executable, script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env
            )
            for byte_line in proc.stdout:
                try:
                    line = byte_line.decode('utf-8')
                except UnicodeDecodeError:
                    line = byte_line.decode('utf-8', errors='replace')
                self.log.append(line.rstrip())
                QApplication.processEvents()

            proc.wait()
            if proc.returncode == 0:
                self.log.append(f"✅ {os.path.basename(script)} 完成\n")
            else:
                self.log.append(f"❌ {os.path.basename(script)} 執行失敗，終止後續！\n")
                break

        self.btn_run.setEnabled(True)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
