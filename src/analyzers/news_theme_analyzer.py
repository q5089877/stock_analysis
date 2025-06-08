import os
import re
import requests
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

import pandas as pd
from transformers import pipeline, AutoTokenizer

from src.utils.config_loader import load_config  # 加上這行以讀取 config


class StockNewsScorer:
    """StockNewsScorer v4 –
    1. 支援多個 RSS 網址，一次抓好幾個新聞來源
    2. 自動截斷超過 512 token 的文字再丟給 FinBERT
    3. 記錄「哪個關鍵字」在「哪篇文章」給了「多少 sentiment×weight」
    4. 輸出熱門產業、總得分、個別新聞得分＋新聞標題的 CSV，並依總得分排序
    """

    def __init__(
        self,
        csv_path: str,
        rss_urls: List[str] = None,
        timeout: int = 10,
        device: int = -1,
    ) -> None:
        self.csv_path = csv_path
        self.timeout = timeout
        self.device = device

        # 處理 rss_urls：如果沒給就使用預設
        if rss_urls is None:
            self.rss_urls = ["https://tw.news.yahoo.com/rss/finance"]
        else:
            if isinstance(rss_urls, str):
                self.rss_urls = [rss_urls]
            else:
                self.rss_urls = rss_urls

        # 讀取股票清單
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"找不到 CSV 檔案：{self.csv_path}")
        df = pd.read_csv(self.csv_path, dtype=str).fillna("")
        if not {"stock_id", "產業別"}.issubset(df.columns):
            raise ValueError("CSV 必須包含 stock_id / 產業別 兩個欄位！")

        # 偵測公司名稱欄位（可選）
        name_cols = [c for c in df.columns if re.search(
            r"公司名稱|有價證券名稱|name", c)]
        self.name_col = name_cols[0] if name_cols else None

        # stock_id → industry / company_name
        self.stock_to_industry: Dict[str, str] = {}
        self.stock_to_name: Dict[str, str] = {}
        for _, row in df.iterrows():
            sid = row["stock_id"].strip()
            ind = row["產業別"].strip()
            self.stock_to_industry[sid] = ind
            if self.name_col:
                name = re.sub(r"股份有限公司$", "", row[self.name_col].strip())
                self.stock_to_name[sid] = name.lower()
            else:
                self.stock_to_name[sid] = ""

        # industry → list of stock_ids
        self.industry_to_stocks: Dict[str, List[str]] = {}
        for sid, ind in self.stock_to_industry.items():
            self.industry_to_stocks.setdefault(ind, []).append(sid)

        # industry → list of keywords (含簡單拆字)
        self.industry_keywords: Dict[str, List[str]] = {}
        suffixes = ["服務業", "業控股", "業務", "業者", "工業", "業"]
        for ind in self.industry_to_stocks:
            base = ind
            for suf in suffixes:
                if base.endswith(suf):
                    base = base[: -len(suf)]
                    break
            parts = [p for p in re.split(
                r"[/、&\s]+", base) if p and len(p) >= 2]
            kws = set([ind] + parts)
            self.industry_keywords[ind] = [k.lower() for k in kws]

        # stock_id → matching keywords (股票代號、公司名稱)
        self.stock_keywords: Dict[str, List[str]] = {}
        for sid, name in self.stock_to_name.items():
            if name:
                self.stock_keywords[sid] = [sid, name]
            else:
                self.stock_keywords[sid] = [sid]

        # 準備 FinBERT tokenizer 和 pipeline
        self.tokenizer = AutoTokenizer.from_pretrained(
            "yiyanghkust/finbert-tone-chinese")
        self.sentiment_classifier = pipeline(
            "sentiment-analysis",
            model="yiyanghkust/finbert-tone-chinese",
            tokenizer=self.tokenizer,
            device=self.device,
        )

    def _fetch_articles(self, max_items_per_source: int = 100) -> List[Dict[str, str]]:
        """
        從多個 RSS 網址下載最新新聞，每個來源最多抓 max_items_per_source 篇
        回傳 list of {
            "title": str,
            "description": str,
            "combined": str
        }
        """
        all_results: List[Dict[str, str]] = []

        for url in self.rss_urls:
            try:
                xml = requests.get(url, timeout=self.timeout).text
                root = ET.fromstring(xml)
            except Exception as e:
                print(f"RSS 取得失敗（{url}）：", e)
                continue

            items_node = root.find("channel")
            items = (
                items_node.findall("item")[:max_items_per_source]
                if items_node is not None
                else []
            )
            for it in items:
                title = (it.findtext("title") or "").strip()
                desc = (it.findtext("description")
                        or it.findtext("summary") or "").strip()
                combined = f"{title} {desc}"
                all_results.append({
                    "title": title,
                    "description": desc,
                    "combined": combined
                })

        return all_results

    @staticmethod
    def _label_to_val(label: str) -> int:
        return 1 if label == "positive" else (-1 if label == "negative" else 0)

    def _get_article_sentiment(self, text: str) -> int:
        """
        先把文字截斷到 512 token，再丟進 FinBERT，回傳 -1, 0, +1
        """
        try:
            encoded = self.tokenizer(
                text,
                truncation=True,
                max_length=512,
                return_tensors="pt",
            )
            truncated_text = self.tokenizer.decode(
                encoded["input_ids"][0], skip_special_tokens=True
            )
            res = self.sentiment_classifier(truncated_text)
            if not res:
                return 0
            return self._label_to_val(res[0]["label"].lower())
        except Exception:
            return 0

    @staticmethod
    def _kw_in_text(kw: str, text: str) -> bool:
        return kw.lower() in text

    @staticmethod
    def _numeric_kw_pattern(num: str) -> re.Pattern:
        return re.compile(rf"[\(（]{num}[\)）]")

    def _score_industries(
        self, articles: List[Dict[str, str]]
    ) -> Tuple[
        Dict[str, float],
        Dict[str, List[Tuple[str, float]]],
        Dict[str, List[Tuple[float, str]]]
    ]:
        """
        articles: list of {"title": str, "description": str, "combined": str}
        回傳：
          - scores: {產業: 0~100}
          - industry_details: {產業: [(reason, contrib), ...]}
          - industry_articles: {產業: [(contrib, title), ...]}
        """
        bucket: Dict[str, List[float]] = {ind: []
                                          for ind in self.industry_keywords}
        industry_details: Dict[str, List[Tuple[str, float]]] = {
            ind: [] for ind in self.industry_keywords
        }
        industry_articles: Dict[str, List[Tuple[float, str]]] = {
            ind: [] for ind in self.industry_keywords
        }

        # 只匹配「(1234)」或「（1234）」格式的股票代號
        stock_code_pattern = re.compile(r"[（(](\d{4})[)）]")

        for item in articles:
            title = item["title"]
            art = item["combined"]
            art_l = art.lower()
            senti = self._get_article_sentiment(art)

            matched_info: Dict[str, List[Tuple[float, str]]] = {}

            # 1️⃣ 找股號（只看括號內 4 位數字）
            for m in stock_code_pattern.finditer(art):
                code = m.group(1)
                if code in self.stock_to_industry:
                    ind = self.stock_to_industry[code]
                    w = 0.8
                    reason = f"(股票代號：{code})"
                    matched_info.setdefault(ind, []).append((w, reason))
                    # 若希望一篇文章只算一次，可加上 break

            # 2️⃣ 公司名稱＋「股份／公司」才算
            for sid, comp in self.stock_to_name.items():
                if comp and comp in art_l:
                    if re.search(rf"{comp}(股份|公司)", art_l):
                        ind = self.stock_to_industry[sid]
                        w = 1.0
                        reason = f"(公司名稱：{comp})"
                        matched_info.setdefault(ind, []).append((w, reason))

            # 3️⃣ 產業關鍵字匹配
            for ind, kws in self.industry_keywords.items():
                for kw in kws:
                    if kw in art_l:
                        w = 0.8
                        reason = f"(產業關鍵字：{kw})"
                        matched_info.setdefault(ind, []).append((w, reason))
                        break

            # 4️⃣ 計算貢獻值並記錄新聞標題
            for ind, pairs in matched_info.items():
                total_w = sum(w for (w, _) in pairs)
                if total_w > 1.0:
                    total_w = 1.0
                if total_w < -1.0:
                    total_w = -1.0
                contrib = senti * total_w
                bucket[ind].append(contrib)
                industry_articles[ind].append((contrib, title))
                for (w, reason) in pairs:
                    industry_details[ind].append((reason, senti * w))

        # 5️⃣ 只算非零貢獻值再平均，若全是 0.0 就給 50
        scores: Dict[str, float] = {}
        for ind, vals in bucket.items():
            nonzero_vals = [v for v in vals if v != 0.0]
            if not nonzero_vals:
                scores[ind] = 50.0
            else:
                avg = sum(nonzero_vals) / len(nonzero_vals)
                scores[ind] = round((avg + 1) / 2 * 100, 2)

        return scores, industry_details, industry_articles

    def score_stocks(self, max_items_per_source: int = 100) -> None:
        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)

        # 1. 抓新聞
        news_list = self._fetch_articles(max_items_per_source)
        if not news_list:
            print("沒有抓到任何新聞。")
            return

        # 2. 計算各產業分數、細節、以及每篇文章貢獻與標題
        ind_scores, industry_details, industry_articles = self._score_industries(
            news_list)

        # 3. 生成「熱門產業 + 各新聞貢獻與標題」的 DataFrame，且只保留 contrib > 0.0
        hot_records = []
        for ind, total_score in ind_scores.items():
            for contrib, title in industry_articles[ind]:
                if contrib > 0.0:
                    hot_records.append({
                        "industry": ind,
                        "total_score": total_score,
                        "news_title": title,
                        "article_score": round(contrib, 2)
                    })

        df_hot = (
            pd.DataFrame(hot_records)
            .sort_values("total_score", ascending=False)
            .reset_index(drop=True)
        )

        hot_csv_path = os.path.join(output_dir, "hot_industry_news.csv")
        df_hot.to_csv(hot_csv_path, index=False, encoding="utf-8-sig")
        print(f"已輸出熱門產業新聞分析 → {hot_csv_path}")


if __name__ == "__main__":
    # 從 config.yaml 讀取 RSS 來源
    cfg = load_config()
    rss_list = cfg.get("rss_sources", [])
    csv_id_path = cfg.get("paths", {}).get("stock_list")
    scorer = StockNewsScorer(csv_path=csv_id_path, rss_urls=rss_list)
    scorer.score_stocks(max_items_per_source=100)
