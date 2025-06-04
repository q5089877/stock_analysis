# theme_tracker.py

from typing import Dict, List


class ThemeTracker:
    """
    ThemeTracker 負責把『題材熱度』換算成『題材分數』，
    讓每支股票都能得到一個 0～100 分的題材面加權分數。
    """

    def __init__(self, keyword_map: Dict[str, List[str]]):
        """
        初始化 ThemeTracker

        參數：
          - keyword_map: dict
              將 每個股票代號 (str) 對應到它屬於的題材列表 (List[str])，例如：
              {
                  "2330": ["半導體", "電子"],
                  "2454": ["電動車", "AI"],
                  "3008": ["生技"],
                  ...
              }
        """
        self.keyword_map = keyword_map

    def get_bonus(self, stock_id: str, theme_hotness: Dict[str, int]) -> float:
        """
        計算單一股票的『題材分數』（0~100）

        輸入：
          - stock_id: str
              股票代號，例如 "2330"
          - theme_hotness: Dict[str, int]
              所有題材的出現次數字典，例如 {"AI": 50, "電動車": 20, "生技": 10, ...}

        輸出：
          - float: 這支股票的題材分數（0~100），如果找不到對應題材，回傳 0.0

        計算邏輯：
          1. 從 keyword_map 拿到這支股票對應的題材列表。
             如果 stock_id 不在 keyword_map 裡，就直接回傳 0.0。
          2. 從 theme_hotness.values() 找出最大熱度 (max_hot) 與最小熱度 (min_hot)。
             如果 max_hot == min_hot，代表所有題材熱度相同（或都為 0），直接回傳 0.0。
          3. 依序把每個相關題材的熱度 (hotness) 轉成「0~100 分」：
             normalized = (hotness - min_hot) / (max_hot - min_hot) * 100
             如果某題材在 theme_hotness 找不到，就視為熱度 0。
          4. 把所有相關題材的 normalized 分數取平均，作為最終題材分數。
        """
        # 1. 檢查股票是否有對應的題材列表
        if stock_id not in self.keyword_map:
            # 如果找不到對應題材，就回傳 0 分
            return 0.0

        related_themes: List[str] = self.keyword_map[stock_id]
        if not related_themes:
            # 如果對應的題材列表為空，也回傳 0 分
            return 0.0

        # 2. 從所有題材熱度中，找出最大熱度與最小熱度
        all_hotness_values = list(theme_hotness.values())
        if not all_hotness_values:
            # 如果 theme_hotness 本身是空的，就回傳 0 分
            return 0.0

        max_hot = max(all_hotness_values)
        min_hot = min(all_hotness_values)

        # 如果最大熱度 == 最小熱度，就表示無法做標準化
        if max_hot == min_hot:
            return 0.0

        # 3. 將每個相關題材的熱度轉成 0~100 的分數
        scores: List[float] = []
        for theme in related_themes:
            # 如果某題材在 theme_hotness 裡沒有，就視為熱度 0
            hotness = theme_hotness.get(theme, 0)
            # 線性轉換公式：normalized = (hotness - min_hot) / (max_hot - min_hot) * 100
            normalized = (hotness - min_hot) / (max_hot - min_hot) * 100
            scores.append(normalized)

        # 4. 取相關題材分數的平均值，作為最終題材分數
        theme_score = sum(scores) / len(scores)
        return theme_score


# 範例測試（可放在另一個測試檔或 main_controller.py 裡執行）
if __name__ == "__main__":
    # 假設 theme_hotness 由 ThemeAnalyzer 算出
    theme_hotness_example = {
        "AI": 50,
        "電動車": 20,
        "生技": 10,
        "電子": 30,
        "半導體": 40
    }

    # 假設 keyword_map 已經建好，將股票對應到題材列表
    keyword_map_example = {
        "2330": ["半導體", "電子"],
        "2454": ["電動車", "AI"],
        "3008": ["生技"],
        "9999": []  # 沒有對應題材
    }

    tracker = ThemeTracker(keyword_map_example)

    for sid in ["2330", "2454", "3008", "9999", "1234"]:
        score = tracker.get_bonus(sid, theme_hotness_example)
        print(f"股票 {sid} 的題材分數：{score:.2f}")
