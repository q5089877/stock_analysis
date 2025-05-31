# src/pipeline/credit_downloader.py

import os
import requests
from urllib.parse import quote


class CreditTwseDownloader:
    """
    下載並儲存 TWSE 融資／融券 CSV 檔案。
    """

    def __init__(self, url_template: str, raw_dir: str):
        self.url_template = url_template
        self.dir = os.path.join(raw_dir, "credit_twse")
        os.makedirs(self.dir, exist_ok=True)

    def download(self, date_str: str) -> str:
        """
        以 date_str (格式 YYYYMMDD) 產生 URL，下載 CSV (UTF-8-sig)，
        存到 raw_dir/credit_twse，並回傳檔案路徑。
        """
        url = self.url_template.format(date=date_str)
        resp = requests.get(url)
        resp.raise_for_status()

        filename = f"twse_credit_{date_str}.csv"
        path = os.path.join(self.dir, filename)
        with open(path, "w", encoding="utf-8-sig") as f:
            f.write(resp.text)
        return path


class CreditTpexDownloader:
    """
    下載並儲存 TPEx 融資／融券 CSV 檔案。
    """

    def __init__(self, url_template: str, raw_dir: str):
        self.url_template = url_template
        self.dir = os.path.join(raw_dir, "credit_tpex")
        os.makedirs(self.dir, exist_ok=True)

    def download(self, date_str: str) -> str:
        """
        以 date_str (格式 YYYYMMDD) 先格式化為 YYYY/MM/DD 並 URL Encode，
        產生 URL，下載 CSV (UTF-8-sig)，存到 raw_dir/credit_tpex，並回傳檔案路徑。
        """
        # 將 "YYYYMMDD" 轉成 "YYYY/MM/DD" 再 URL encode
        date_fmt = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        date_url = quote(date_fmt, safe="")
        url = self.url_template.format(date_url=date_url)
        resp = requests.get(url)
        resp.raise_for_status()

        filename = f"tpex_credit_{date_str}.csv"
        path = os.path.join(self.dir, filename)
        with open(path, "w", encoding="utf-8-sig") as f:
            f.write(resp.text)
        return path


if __name__ == "__main__":
    # 獨立測試
    from src.utils.config_loader import load_config
    cfg = load_config()
    raw_dir = cfg["paths"]["raw_data"]

    dt = "20250527"
    # 測試 TWSE 融資/融券 CSV 下載
    tw_down = CreditTwseDownloader(
        cfg["credit"]["twse"]["url_template"], raw_dir)
    print(f"下載 TWSE 融資/融券 CSV: {tw_down.download(dt)}")

    # 測試 TPEx 融資/融券 CSV 下載
    tp_down = CreditTpexDownloader(
        cfg["credit"]["tpex"]["url_template"], raw_dir)
    print(f"下載 TPEx 融資/融券 CSV: {tp_down.download(dt)}")
