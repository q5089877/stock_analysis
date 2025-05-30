# src/pipeline/ticket_downloader.py

import os
import requests
from urllib.parse import quote
from src.utils.config_loader import load_config


class TicketTwseScraper:
    """
    下載並儲存 TWSE 融券/借券 HTML 原始檔案。
    """

    def __init__(self, url_template: str, raw_dir: str):
        self.url_template = url_template
        # 建立 TWSE 資料夾
        self.dir = os.path.join(raw_dir, "ticket_twse")
        os.makedirs(self.dir, exist_ok=True)

    def fetch(self, date_str: str) -> str:
        """
        以 date_str (YYYYMMDD) 產生網址並下載 HTML，存到 raw_dir/ticket_twse，回傳檔案路徑。
        """
        # TWSE 網址直接用 date_str
        url = self.url_template.format(date=date_str)
        resp = requests.get(url)
        resp.raise_for_status()

        filename = f"ticket_twse_{date_str}.html"
        path = os.path.join(self.dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return path


class TicketTpexScraper:
    """
    下載並儲存 TPEx 融券/借券 HTML 原始檔案。
    """

    def __init__(self, url_template: str, raw_dir: str):
        self.url_template = url_template
        # 建立 TPEX 資料夾
        self.dir = os.path.join(raw_dir, "ticket_tpex")
        os.makedirs(self.dir, exist_ok=True)

    def fetch(self, date_str: str) -> str:
        """
        以 date_str (YYYYMMDD) 產生 URL-encoded 日期字串並下載 HTML，
        存到 raw_dir/ticket_tpex，回傳檔案路徑。
        """
        # 將 "20250528" 轉成 "2025/05/28" 再 URL Encode
        date_fmt = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        date_url = quote(date_fmt, safe="")
        url = self.url_template.format(date_url=date_url)
        resp = requests.get(url)
        resp.raise_for_status()

        filename = f"ticket_tpex_{date_str}.html"
        path = os.path.join(self.dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return path


if __name__ == "__main__":
    # 獨立測試入口
    cfg = load_config()
    raw_dir = cfg["paths"]["raw_data"]

    test_date_twse = "20250522"
    tw_scraper = TicketTwseScraper(cfg["ticket"]["twse_url_template"], raw_dir)
    tw_path = tw_scraper.fetch(test_date_twse)
    print(f"[測試] TWSE Ticket 已下載並儲存到：{tw_path}")

    test_date_tpex = "20250528"
    tp_scraper = TicketTpexScraper(cfg["ticket"]["tpex_url_template"], raw_dir)
    tp_path = tp_scraper.fetch(test_date_tpex)
    print(f"[測試] TPEx Ticket 已下載並儲存到：{tp_path}")
