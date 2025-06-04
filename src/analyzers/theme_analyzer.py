# src/analyzers/theme_analyzer.py

from transformers import pipeline


class ThemeAnalyzer:
    """
    ThemeAnalyzer 這個版本改用 FinBERT (yiyanghkust/finbert-tone) 做財經新聞情感分析。

    主要功能：
      1. 根據 keyword_map（題材對應的關鍵字列表），掃描每則新聞文章，
         計算各題材在文章中出現的次數（theme_hotness）。
      2. 當發現一篇文章屬於某個題材時，用 FinBERT 進行情感分析，
         統計該篇文章對應題材是「正面/中立」還是「負面」（theme_sentiment）。

    屬性：
      - keyword_map: dict[str, list[str]]
          將『題材』對應到一串關鍵字。例如：
            {
              "AI": ["AI", "人工智慧", "機器學習"],
              "電動車": ["電動車", "Tesla", "特斯拉"],
              "生技": ["生技", "製藥", "醫藥"],
              "電子": ["電子", "半導體", "晶圓"],
              "半導體": ["半導體", "IC", "晶片"],
            }

      - theme_sentiment: dict[str, dict[str, int]]
          儲存每個題材的情感計數，例如：
            {
              "AI": {"positive": 0, "negative": 0},
              "電動車": {"positive": 0, "negative": 0},
              ...
            }
          其中「positive」計算正面／中立篇數，「negative」計算負面篇數。

      - sentiment_classifier: transformers pipeline
          用來做情感分析的 FinBERT 模型（yiyanghkust/finbert-tone）。

    方法：
      - analyze(articles) → (theme_hotness, theme_sentiment)
          對給定的文章列表做統計，回傳：
            * theme_hotness: {題材: 出現次數}
            * theme_sentiment: {題材: {"positive": 篇數, "negative": 篇數}}
    """

    def __init__(self, keyword_map: dict, model_name: str = "yiyanghkust/finbert-tone"):
        """
        初始化 ThemeAnalyzer。

        參數：
          - keyword_map: dict
              將每個題材對應到一串關鍵字（List[str]）。
          - model_name: str
              用於情感分析的模型名稱，預設為 "yiyanghkust/finbert-tone"（FinBERT）。
        """
        # 存放題材與關鍵字列表的對照表
        self.keyword_map = keyword_map

        # 初始化 theme_sentiment，先把每個題材的正負面計數設為 0
        # 這樣之後分析就能直接累加
        self.theme_sentiment = {
            theme: {"positive": 0, "negative": 0}
            for theme in keyword_map
        }

        # 建立 FinBERT pipeline，用於針對文章做「情感分析」
        # FinBERT 回傳的標籤有 "positive"/"neutral"/"negative"，
        # 我們把「neutral」歸到「positive」裡（視為正面或中立）。
        self.sentiment_classifier = pipeline(
            "sentiment-analysis",
            model=model_name,
            tokenizer=model_name
        )

    def analyze(self, articles: list[str]) -> (dict[str, int], dict[str, dict[str, int]]):
        """
        分析一串財經新聞文章，回傳『題材熱度』和『題材情感』。

        輸入：
          - articles: list[str]
              一個 list，裡面每個元素都是一篇新聞文章的完整文字。

        輸出：
          - theme_hotness: dict[str, int]
              每個題材出現的次數。例如：
                {
                  "AI": 50,
                  "電動車": 20,
                  "生技": 10,
                  ...
                }

          - theme_sentiment: dict[str, {"positive": int, "negative": int}]
              每個題材的情感計數。例如：
                {
                  "AI": {"positive": 40, "negative": 10},
                  "電動車": {"positive": 15, "negative": 5},
                  ...
                }

        處理流程：
          1. 先把 theme_sentiment 裡的「positive/negative」都歸零。
          2. 初始化 theme_hotness，讓每個題材的次數從 0 開始。
          3. 對每一篇文章 article：
             a. 依序檢查每個題材 (theme) 以及它的關鍵字列表 (keywords)：
                - 如果文章裡有出現該題材下任一關鍵字 (kw)，就：
                  1. 在 theme_hotness[theme] 上 +1（代表題材出現一次）。
                  2. 用 FinBERT 做該篇文章的情感分析 (sentiment_classifier(article))：
                     * 如果結果是 "negative"，就把 theme_sentiment[theme]["negative"] +1。
                     * 否則（"positive" 或 "neutral"），就把 theme_sentiment[theme]["positive"] +1。
                  3. 一篇文章只算一次同一題材，所以 break 跳出關鍵字迴圈。
             b. 如果文章沒與任何關鍵字比對，該篇文章對那個題材就不加分也不計情感。

          4. 回傳最終的 theme_hotness 與 theme_sentiment。

        注意：
          - 如果某篇文章同時包含題材 A 和題材 B 的關鍵字，
            在走迴圈時，會先對 A 做檢查，若匹配到，就先算 A 的次數與情感，不繼續檢查 A 底下的其餘關鍵字，但仍會繼續往 B 檢查（因為是兩個不同題材）。
          - 如果要更嚴謹避免一篇文章同時被歸到同一題材多次，
            我們在找到第一個關鍵字匹配時，就對該題材 break；但文章還是會去嘗試其他題材關鍵字。
        """
        # 1. 歸零每個題材的情感計數
        for theme in self.keyword_map:
            self.theme_sentiment[theme]["positive"] = 0
            self.theme_sentiment[theme]["negative"] = 0

        # 2. 初始化 theme_hotness，先讓每個題材次數都從 0 開始
        theme_hotness = {theme: 0 for theme in self.keyword_map}

        # 3. 開始對每篇文章做分析
        for article in articles:
            # 3a. 對每個題材和它的關鍵字列表做比對
            for theme, keywords in self.keyword_map.items():
                # 檢查這篇文章是否包含該題材的任何關鍵字
                matched = False
                for kw in keywords:
                    if kw in article:
                        matched = True
                        break  # 找到一個關鍵字就算這篇文章屬於該題材
                if matched:
                    # 3a-1. 題材出現次數 +1
                    theme_hotness[theme] += 1

                    # 3a-2. 用 FinBERT 做情感分析
                    result = self.sentiment_classifier(article)[0]
                    # 會是 "positive", "neutral" 或 "negative"
                    label = result["label"]
                    if label == "negative":
                        self.theme_sentiment[theme]["negative"] += 1
                    else:
                        # "positive" 或 "neutral" 都算在 positive 裡
                        self.theme_sentiment[theme]["positive"] += 1

                    # 3a-3. 一篇文章只算一次同一題材，所以 break 掉 keywords 的迴圈
                    #      但分析其他題材時，還是會繼續檢查
                    #      所以這裡不跳出最外層迴圈，只跳出 keywords 迴圈
                    continue

        # 4. 回傳所有題材的熱度與情感統計
        return theme_hotness, self.theme_sentiment


# 以下是測試範例（只在直接執行此檔時才跑）
if __name__ == "__main__":
    # 範例 keyword_map：將題材對應到可能出現的關鍵字
    keyword_map = {
        "AI": ["AI", "人工智慧", "機器學習"],
        "電動車": ["電動車", "Tesla", "特斯拉"],
        "生技": ["生技", "製藥", "醫藥"],
        "電子": ["電子", "半導體", "晶圓"],
        "半導體": ["半導體", "IC", "晶片"]
    }

    # 假設我們有一些測試用的新聞文章
    dummy_articles = [
        "蘋果投資 AI 研發團隊，加速人工智慧產品布局。",
        "Tesla 宣布新款電動車，特斯拉股價攀升。",
        "晶片廠商 2330 發表新一代半導體技術。",
        "XYZ 生技公司 ABC 的新藥通過 FDA 審查，生技股瞬間爆發。",
        "微軟推出新 AI 平台，人工智慧應用更廣泛，投資人看好未來前景。",
        "生技公司 ABC 的新藥通過 FDA 審查，生技股瞬間爆發。"
    ]

    ta = ThemeAnalyzer(keyword_map)
    hotness, sentiment = ta.analyze(dummy_articles)

    print("=== 題材熱度 (theme_hotness) ===")
    for theme, cnt in hotness.items():
        print(f"{theme}: {cnt}")

    print("\n=== 題材情感 (theme_sentiment) ===")
    for theme, stats in sentiment.items():
        print(
            f"{theme}: 正面(含中立) {stats['positive']} 篇，負面 {stats['negative']} 篇")
