# src/analyzers/news_fetcher.py

import requests
import xml.etree.ElementTree as ET


class NewsFetcher:
    """
    改用內建函式庫解析 RSS，不需要安裝外部套件
    1. 用 requests.get() 去取得 RSS XML 資料
    2. 用 xml.etree.ElementTree 解析 XML，取出 <item> 裡的 <title> 和 <description>
    3. 回傳 list，每個元素都是「標題 + 內文摘要」字串
    """

    def __init__(self, rss_url: str):
        self.rss_url = rss_url

    def fetch(self) -> list:
        """
        連到 RSS 網址，拿到一整段 XML 字串
        再用 ElementTree 解析，取出前幾篇 <item> 裡的標題和內文摘要
        回傳一個 list，裡面每個元素都是 "標題 內文摘要" 的字串
        """
        articles = []

        try:
            # 1. 用 requests 拿 RSS 裡的 XML
            response = requests.get(self.rss_url, timeout=10)
            response.raise_for_status()  # 如果 HTTP 出錯會跳例外
            xml_content = response.text  # 取得 RSS XML 原始文字
        except Exception as e:
            print("無法取得 RSS，原因：", e)
            return articles

        try:
            # 2. 用 ElementTree 解析 XML
            root = ET.fromstring(xml_content)
            # RSS 的結構通常是 <rss><channel><item>...</item><item>...</item>...</channel></rss>
            # 所以我們先找到 <channel>，再對每個 <item> 迭代
            channel = root.find("channel")
            if channel is None:
                return articles

            for item in channel.findall("item"):
                # 取 <title> 的文字
                title_elem = item.find("title")
                title = title_elem.text.strip() if title_elem is not None else ""

                # 取 <description>（有時候寫在 <description>，有時候 RSS 會叫它 <summary>）
                desc_elem = item.find("description")
                if desc_elem is None:
                    desc_elem = item.find("summary")
                summary = desc_elem.text.strip() if desc_elem is not None else ""

                # 合併成一段長文字
                combined = f"{title} {summary}"
                articles.append(combined)

            return articles

        except Exception as e:
            print("解析 RSS XML 出錯，原因：", e)
            return articles


if __name__ == "__main__":
    # 測試程式：執行這支檔案就會跑下面的程式

    # 1. 設定 RSS 網址：這裡示範用 Yahoo 股市財經 RSS
    rss_url = "https://tw.news.yahoo.com/rss/finance"

    # 2. 建立 NewsFetcher 物件
    nf = NewsFetcher(rss_url)

    # 3. 呼叫 fetch() 抓新聞
    articles = nf.fetch()

    # 4. 印出抓到的篇數
    print(f"抓到的新聞總數：{len(articles)} 篇\n")

    # 5. 印出前 5 篇的前 200 字（如果有超過 5 篇的話）
    max_to_show = 5
    for i, text in enumerate(articles[:max_to_show], start=1):
        print(f"--- 第 {i} 篇新聞 ---")
        print(text[:200] + "...\n")
