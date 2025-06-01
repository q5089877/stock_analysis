import sqlite3

# 改成你自己的 sqlite 檔完整路徑
sqlite_path = r"C:/Users/q5089/Desktop/stock_analysis/db/stockDB.db"
conn = sqlite3.connect(sqlite_path)
cur = conn.cursor()

# 1) 檢查 ticket_twse 表欄位
print("=== ticket_twse 欄位與範例資料 ===")
cur.execute("PRAGMA table_info(ticket_twse)")
for col in cur.fetchall():
    # col 會是一筆 tuple： (cid, name, type, notnull, dflt_value, pk)
    print(col)

# 2) 列出 ticket_twse 裡前 5 筆的「日期」欄位值
#    先測試 SELECT date
try:
    cur.execute("SELECT date FROM ticket_twse LIMIT 5")
    print("date 欄位值範例：", cur.fetchall())
except Exception as e:
    print("沒有叫 date 這個欄位：", e)

#    再測試 SELECT Date
try:
    cur.execute("SELECT Date FROM ticket_twse LIMIT 5")
    print("Date 欄位值範例：", cur.fetchall())
except Exception as e:
    print("沒有叫 Date 這個欄位：", e)

#    再測試 SELECT `日期`（中文）
try:
    cur.execute("SELECT 日期 FROM ticket_twse LIMIT 5")
    print("日期 欄位值範例：", cur.fetchall())
except Exception as e:
    print("沒有叫 日期 這個欄位：", e)

print("\n=== ticket_tpex 欄位與範例資料 ===")
cur.execute("PRAGMA table_info(ticket_tpex)")
for col in cur.fetchall():
    print(col)

# 列出 ticket_tpex 裡前 5 筆的「日期」欄位值
try:
    cur.execute("SELECT date FROM ticket_tpex LIMIT 5")
    print("date 欄位值範例：", cur.fetchall())
except Exception as e:
    print("ticket_tpex 沒有叫 date 這個欄位：", e)

try:
    cur.execute("SELECT Date FROM ticket_tpex LIMIT 5")
    print("Date 欄位值範例：", cur.fetchall())
except Exception as e:
    print("ticket_tpex 沒有叫 Date 這個欄位：", e)

try:
    cur.execute("SELECT 日期 FROM ticket_tpex LIMIT 5")
    print("日期 欄位值範例：", cur.fetchall())
except Exception as e:
    print("ticket_tpex 沒有叫 日期 這個欄位：", e)

conn.close()
