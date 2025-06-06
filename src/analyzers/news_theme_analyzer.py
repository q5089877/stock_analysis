import os
import re
import requests
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

import pandas as pd
from transformers import pipeline, AutoTokenizer


class StockNewsScorer:
    """StockNewsScorer v3 –
    1. 自動截斷超過 512 token 的文字再丟給 FinBERT
    2. 記錄「哪個關鍵字」在「哪篇文章」給了「多少 sentiment×weight」
    3. 輸出最終每檔股票分數 + 詳細加分原因到 CSV
    4. 另將抓到的所有新聞 (title, description) 也輸出到 CSV
    5. 產生熱門產業與熱門股票報告
    """

    def __init__(
        self,
        csv_path: str,
        rss_url: str = "https://tw.news.yahoo.com/rss/finance",
        timeout: int = 10,
        device: int = -1,
    ) -> None:
        self.csv_path = csv_path
        self.rss_url = rss_url
        self.timeout = timeout
        self.device = device

        # --- 讀取股票清單 --- #
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(self.csv_path)
        df = pd.read_csv(self.csv_path, dtype=str).fillna("")
        if not {"stock_id", "產業別"}.issubset(df.columns):
            raise ValueError("CSV 缺少 stock_id / 產業別 欄位！")

        # 偵測「公司名稱」欄位
        name_cols = [
            c
            for c in df.columns
            if re.search(r"公司名稱|有價證券名稱|name", c)
        ]
        self.name_col = name_cols[0] if name_cols else None
        if not self.name_col:
            print("⚠️ CSV 無公司名稱欄，僅能用產業/股號比對")

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

        # industry → list of stock_ids
        self.industry_to_stocks: Dict[str, List[str]] = {}
        for sid, ind in self.stock_to_industry.items():
            self.industry_to_stocks.setdefault(ind, []).append(sid)

        # 針對每個 industry，拆出「產業關鍵字 (長度 >= 2)」
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

        # 針對每檔股票，只用「股票代號 + 完整公司名稱」當作 matching keywords
        self.stock_keywords: Dict[str, List[str]] = {}
        for sid, name in self.stock_to_name.items():
            if name:
                self.stock_keywords[sid] = [sid, name]
            else:
                self.stock_keywords[sid] = [sid]

        # --- 準備 tokenizer & FinBERT pipeline --- #
        self.tokenizer = AutoTokenizer.from_pretrained(
            "yiyanghkust/finbert-tone-chinese"
        )
        self.sentiment_classifier = pipeline(
            "sentiment-analysis",
            model="yiyanghkust/finbert-tone-chinese",
            tokenizer=self.tokenizer,
            device=device,
        )

    # ------------------------- 抓新聞 (返回明細) ------------------------- #
    def _fetch_articles(self, max_items: int = 200) -> List[Dict[str, str]]:
        """
        從 RSS 下載最新max_items篇新聞，回傳 list of {
            "title": str,
            "description": str,
            "combined": str  # title + " " + description
        }
        """
        try:
            xml = requests.get(self.rss_url, timeout=self.timeout).text
            root = ET.fromstring(xml)
        except Exception as e:
            print("RSS 取得失敗：", e)
            return []

        items_node = root.find("channel")
        items = (
            items_node.findall("item")[:max_items]
            if items_node is not None
            else []
        )
        results: List[Dict[str, str]] = []
        for it in items:
            title = (it.findtext("title") or "").strip()
            desc = (it.findtext("description")
                    or it.findtext("summary") or "").strip()
            combined = f"{title} {desc}"
            results.append({
                "title": title,
                "description": desc,
                "combined": combined
            })
        return results

    # ------------------------- 情緒推論 ------------------------- #
    @staticmethod
    def _label_to_val(label: str) -> int:
        return 1 if label == "positive" else (-1 if label == "negative" else 0)

    def _get_article_sentiment(self, text: str) -> int:
        """
        先把 text 截斷到 512 token，再丟進 FinBERT 取得 -1/0/+1。
        若失敗則回傳 0（中立）。
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

    # ------------------------- 關鍵字比對 ------------------------- #
    @staticmethod
    def _kw_in_text(kw: str, text: str) -> bool:
        return kw.lower() in text

    @staticmethod
    def _numeric_kw_pattern(num: str) -> re.Pattern:
        return re.compile(rf"[\(（]{num}[\)）]")

    # ------------------------- 主計算：算各產業分數並收集詳情 ------------------------- #
    def _score_industries(
        self, articles: List[str]
    ) -> Tuple[Dict[str, float], Dict[str, List[Tuple[str, float]]]]:
        """
        articles: 只要是文章完整文字 (title+desc)，單純 list of str
        回傳:
            scores: {產業: 0~100 分數}
            industry_details: {產業: [(reason, contrib), ...]}
        """
        bucket: Dict[str, List[float]] = {ind: []
                                          for ind in self.industry_keywords}
        industry_details: Dict[str, List[Tuple[str, float]]] = {
            ind: [] for ind in self.industry_keywords
        }

        for art in articles:
            art_l = art.lower()
            senti = self._get_article_sentiment(art)

            # 改 matched_info 為：key=產業, value=list of (weight, reason)
            matched_info: Dict[str, List[Tuple[float, str]]] = {}

            # 1️⃣ 公司名稱 / 股票代號
            for sid, kws in self.stock_keywords.items():
                ind = self.stock_to_industry[sid]
                for kw in kws:
                    if kw.isdigit():  # 純數字代號
                        if self._numeric_kw_pattern(kw).search(art):
                            w = 0.8
                            reason = f"(股票代號：{kw})"
                            matched_info.setdefault(
                                ind, []).append((w, reason))
                            break
                    else:  # 公司名稱
                        if self._kw_in_text(kw, art_l):
                            w = 1.0
                            reason = f"(公司名稱：{kw})"
                            matched_info.setdefault(
                                ind, []).append((w, reason))
                            break

            # 2️⃣ 產業關鍵字
            for ind, kws in self.industry_keywords.items():
                for kw in kws:
                    if self._kw_in_text(kw, art_l):
                        w = 0.8
                        reason = f"(產業關鍵字：{kw})"
                        matched_info.setdefault(ind, []).append((w, reason))
                        break

            # 3️⃣ 對 matched_info 裡的每個產業，先算 sum(weights)，再 cap 到 [-1, +1]
            for ind, pairs in matched_info.items():
                total_w = sum(w for (w, _) in pairs)
                if total_w > 1.0:
                    total_w = 1.0
                if total_w < -1.0:
                    total_w = -1.0
                contrib = senti * total_w
                bucket[ind].append(contrib)
                # 同時把各原因的貢獻寫進 details
                for (w, reason) in pairs:
                    industry_details[ind].append((reason, senti * w))

        # 計算 0~100 分數
        scores: Dict[str, float] = {}
        for ind, vals in bucket.items():
            if not vals:
                scores[ind] = 50.0
            else:
                avg = sum(vals) / len(vals)
                scores[ind] = round((avg + 1) / 2 * 100, 2)
        return scores, industry_details

    # ------------------------- 對外接口：將新聞和配對結果一起輸出到 CSV ------------------------- #
    def score_stocks(self, max_items: int = 200) -> None:
        # --- 1. 抓新聞原始明細 --- #
        news_list = self._fetch_articles(max_items)
        if not news_list:
            print("沒有抓到任何新聞。")
            return

        # 把「新聞明細」寫到 news_list.csv
        df_news = pd.DataFrame([{
            "title": item["title"],
            "description": item["description"]
        } for item in news_list])
        df_news.to_csv("news_list.csv", index=False, encoding="utf-8-sig")
        print(f"已把 {len(df_news)} 篇新聞寫到 news_list.csv。")

        # --- 2. 用「合併文字」計算各產業情緒分數 --- #
        combined_texts = [item["combined"] for item in news_list]
        ind_scores, industry_details = self._score_industries(combined_texts)

        # --- 3. 把分數套到每支股票 --- #
        stock_scores: Dict[str, float] = {}
        for ind, stocks in self.industry_to_stocks.items():
            for sid in stocks:
                stock_scores[sid] = ind_scores[ind]

        # --- 4. 把 matching details 套到每支股票 --- #
        matched_stocks_details: Dict[str, List[Tuple[str, float]]] = {
            sid: [] for sid in self.stock_to_industry
        }
        for ind, details in industry_details.items():
            for sid in self.industry_to_stocks[ind]:
                matched_stocks_details[sid] = details.copy()

        # --- 5. 準備輸出股票分數 & 細節 --- #
        records = []
        for sid, score in stock_scores.items():
            ind = self.stock_to_industry[sid]
            details_list = matched_stocks_details[sid]
            if details_list:
                detail_strs = [
                    f"{reason}，貢獻={contrib}" for (reason, contrib) in details_list
                ]
                detail_field = "；".join(detail_strs)
                matched_flag = True
            else:
                detail_field = ""
                matched_flag = False

            records.append({
                "stock_id": sid,
                "industry": ind,
                "final_score": score,
                "matched": matched_flag,
                "details": detail_field,
            })

        df_out = pd.DataFrame.from_records(records)
        df_out.to_csv("stock_scores_with_reasons.csv",
                      index=False, encoding="utf-8-sig")
        print(f"已把 {len(df_out)} 檔股票結果寫到 stock_scores_with_reasons.csv。")

        # --- 6. 產生熱門產業與熱門股票報告 --- #
        # 熱門產業：按分數由高到低排序
        hot_inds = sorted(ind_scores.items(), key=lambda x: x[1], reverse=True)
        df_inds = pd.DataFrame(hot_inds, columns=["category", "score"])
        df_inds.insert(0, "type", "industry")
        df_inds.rename(columns={"category": "industry_name"}, inplace=True)

        # 熱門股票：按分數由高到低排序
        hot_stocks = sorted(stock_scores.items(),
                            key=lambda x: x[1], reverse=True)
        # 取得公司名稱
        hot_stock_records = []
        for sid, sc in hot_stocks:
            name = self.stock_to_name.get(sid, "")
            hot_stock_records.append((sid, name, sc))
        df_stks = pd.DataFrame(hot_stock_records, columns=[
                               "stock_id", "company_name", "score"])
        df_stks.insert(0, "type", "stock")

        # 合併成一份報告 CSV
        df_report = pd.concat([
            df_inds.rename(columns={"industry_name": "id", "score": "score"}),
            df_stks.rename(columns={"stock_id": "id", "score": "score"})
        ], ignore_index=True)
        df_report.to_csv("hot_report.csv", index=False, encoding="utf-8-sig")
        print(f"已把熱門產業和熱門股票寫到 hot_report.csv。共 {len(df_report)} 筆。")

        # --- 7. 顯示前 10 筆示範 --- #
        print("\n=== 前 10 筆熱門產業/股票範例 ===")
        print(df_report.head(10))


if __name__ == "__main__":
    scorer = StockNewsScorer("data/stock_id/stock_id.csv")
    scorer.score_stocks(max_items=200)
