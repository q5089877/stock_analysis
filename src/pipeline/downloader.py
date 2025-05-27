import datetime
import os
import time
import requests
import pandas as pd
from io import StringIO
from datetime import datetime


class TWSEDownloader:
    def __init__(self, url_template: str, save_dir: str):
        self.url_template = url_template
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def download(self, date: str):
        """
        下載指定日期的 TWSE 原始 CSV 檔
        :param date: 格式為 YYYYMMDD
        :return: 解碼後的文字內容
        """
        url = self.url_template.format(date=date)
        response = requests.get(url)

        # 強制以 Big5 解碼（TWSE 實際上常是 Big5）
        text = response.content.decode("big5", errors="ignore")

        filename = f"twse_{date}.csv"
        file_path = os.path.join(self.save_dir, filename)

        # 儲存為 UTF-8 with BOM，確保 Excel 能讀
        with open(file_path, "w", encoding="utf-8-sig") as f:
            f.write(text)

        print(f"✅ TWSE 資料已儲存：{file_path}")
        return text  # ✅ 加上這一行，主流程才能用來判斷是否為空資料


class TPExDownloader:

    def __init__(self, url_template: str, save_dir: str):
        self.url_template = url_template
        self.save_dir = save_dir
        os.makedirs(os.path.join(self.save_dir, "tpex"), exist_ok=True)

    def _to_roc_date(self, date: str) -> str:
        dt = datetime.strptime(date, "%Y%m%d")
        roc_year = dt.year - 1911
        return f"{roc_year}/{dt.month:02d}/{dt.day:02d}"

    def download(self, date: str):
        print(date)
        url = self.url_template.format(date=date)
        response = requests.get(url)
        text = response.content.decode("big5", errors="ignore")

        filename = f"tpex_{date}.csv"
        file_path = os.path.join(self.save_dir, "tpex", filename)

        with open(file_path, "w", encoding="utf-8-sig") as f:
            f.write(text)

        print(f"✅ TPEx 資料已儲存：{file_path}")


class InstitutionalTWSEDownloader:
    def __init__(self, url_template: str, save_root: str):
        """
        save_root: 傳入 data/raw，實際儲存到 data/raw/twse_institutional/
        """
        self.url_template = url_template
        self.save_dir = os.path.join(save_root, "twse_institutional")
        os.makedirs(self.save_dir, exist_ok=True)

    def download(self, date: str) -> str:
        """
        下載指定日期的 TWSE 法人買賣超資料（T86）
        :param date: 格式為 YYYYMMDD
        :return: 儲存檔案路徑
        """
        url = self.url_template.format(date=date)
        response = requests.get(url)
        text = response.content.decode("big5", errors="ignore")

        filename = f"twse_institutional_{date}.csv"
        file_path = os.path.join(self.save_dir, filename)

        with open(file_path, "w", encoding="utf-8-sig") as f:
            f.write(text)

        print(f"✅ TWSE 法人資料已儲存：{file_path}")
        return file_path


class TPExInstitutionalDownloader:
    def __init__(self, save_dir: str):
        self.url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.tpex.org.tw/web/stock/3insti/"
        }
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric = (
            df.iloc[:, 2:]
              .replace(',', '', regex=True)
              .apply(pd.to_numeric, errors='coerce')
              .fillna(0)
              .astype(int)
        )
        return pd.concat([df.iloc[:, :2], numeric], axis=1)

    def download(self, roc_date: str) -> pd.DataFrame:
        """
        下載指定日期的 TPEX 法人資料（民國格式日期，如 114/05/08）
        """
        params = {
            "l": "zh-tw",
            "d": roc_date,
            "se": "AL",
            "t": "D"
        }

        for _ in range(3):  # retry 最多 3 次
            try:
                r = requests.get(self.url, params=params,
                                 headers=self.headers, timeout=10)
                r.raise_for_status()
                js = r.json()
                table = (js.get('tables') or [None])[0]
                if not table:
                    raise ValueError("無法取得 table 欄位")

                df = pd.DataFrame(table.get('data'),
                                  columns=table.get('fields'))
                df = self.clean_data(df)

                # 欄位重命名
                rename_map = {
                    2: '外資_買進',  3: '外資_賣出',   4: '外資_買賣超',
                    11: '投信_買進', 12: '投信_賣出', 13: '投信_買賣超',
                    20: '自營_買進', 21: '自營_賣出', 22: '自營_買賣超'
                }
                cols = df.columns.tolist()
                for idx, new_col in rename_map.items():
                    if idx < len(cols):
                        cols[idx] = new_col
                df.columns = cols

                # 統一欄位與格式
                df.insert(0, '市場', 'TPEX')
                df.rename(
                    columns={df.columns[1]: '證券代號', df.columns[2]: '證券名稱'}, inplace=True)
                df = df[['市場', '證券代號', '證券名稱',
                         '外資_買進', '外資_賣出', '外資_買賣超',
                         '投信_買進', '投信_賣出', '投信_買賣超',
                         '自營_買進', '自營_賣出', '自營_買賣超']]

                # 儲存 CSV
                file_name = f"tpex_institutional_{roc_date.replace('/', '')}.csv"
                file_path = os.path.join(self.save_dir, file_name)
                df.to_csv(file_path, index=False, encoding="utf-8-sig")
                print(f"✅ TPEX 法人資料已儲存：{file_path}")
                return df
            except Exception as e:
                print(f"⚠️ 下載失敗重試中：{e}")
                time.sleep(1)

        raise ValueError(f"❌ 下載失敗（已重試 3 次）：{roc_date}")

        return df


class TWSEPEDownloader:
    URL = "https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date={date}&selectType=ALL"

    def __init__(self, save_dir: str):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def download(self, date: str) -> str:
        """
        下載指定日期的本益比 CSV，存成 twse_pe_{date}.csv
        :return: 完整檔案路徑
        """
        url = self.URL.format(date=date)
        resp = requests.get(url)
        resp.raise_for_status()
        text = resp.content.decode("big5", errors="ignore")

        # 只取「逗點數大於 5」的行
        lines = [ln for ln in text.splitlines() if ln.count(",") > 5]
        content = "\n".join(lines)

        fn = f"twse_pe_{date}.csv"
        path = os.path.join(self.save_dir, fn)
        with open(path, "w", encoding="utf-8-sig") as f:
            f.write(content)

        print(f"✅ PE CSV 已存：{path}")
        return path


class TPEXPEDownloader:
    URL = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=csv&charset=UTF-8&d={date}&c=&s=0,asc"

    def __init__(self, save_dir: str):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def download(self, date: str) -> str:
        """
        下載指定日期的上櫃本益比 CSV，存成 tpex_pe_{date}.csv
        :param date: 格式 YYYYMMDD
        :return: 檔案路徑
        """
        url = self.URL.format(date=date)
        resp = requests.get(url)
        resp.raise_for_status()
        text = resp.content.decode("big5", errors="ignore")

        # 只保留欄位數超過 5 的行
        lines = [ln for ln in text.splitlines() if ln.count(",") > 5]
        content = "\n".join(lines)

        filename = f"tpex_pe_{date}.csv"
        path = os.path.join(self.save_dir, filename)
        with open(path, "w", encoding="utf-8-sig") as f:
            f.write(content)

        print(f"✅ TPEx PE CSV 已存：{path}")
        return path
