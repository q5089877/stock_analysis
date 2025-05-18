import pandas as pd

df_csv = pd.read_csv('data/stock_id/stock_id.csv', encoding='utf-8-sig')
print(df_csv.columns)  # 檢查欄位名稱
print(df_csv.head())   # 檢查內容

# 確認可以正常存取
stock_ids = df_csv['stock_id'].astype(str).tolist()
print(stock_ids)
