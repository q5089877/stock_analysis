from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
from io import StringIO


def fetch_quarterly_table(stock_id):
    url = f"https://concords.moneydj.com/z/zc/zce/zce_{stock_id}.djhtm"
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    table = soup.find("table", {"id": "oMainTable"})
    if table is None:
        print("找不到 oMainTable")
        return None

    # 手動 parse，避免多餘的 row 亂入
    headers = [
        "quarter", "revenue", "cost", "gross_profit", "gross_margin",
        "operating_profit", "operating_margin", "non_operating",
        "pretax_profit", "net_profit", "eps"
    ]
    rows = []
    for tr in table.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        # 只保留像 "114.1Q" 這種 row
        if tds and len(tds) == 11 and "Q" in tds[0]:
            rows.append(tds)

    if not rows:
        print("沒有解析到季度數據！")
        return None

    df = pd.DataFrame(rows, columns=headers)
    # 轉型數值型欄位
    for col in ["revenue", "cost", "gross_profit", "operating_profit", "pretax_profit", "net_profit"]:
        df[col] = df[col].str.replace(",", "").replace(
            "-", "0").astype(float).astype(int)
    df["eps"] = df["eps"].str.replace(",", "").replace("-", "0").astype(float)
    df["quarter"] = df["quarter"].str.strip()

    print(df.head())  # 看一下結果
    driver.quit()
    return df


if __name__ == "__main__":
    fetch_quarterly_table("2330")
