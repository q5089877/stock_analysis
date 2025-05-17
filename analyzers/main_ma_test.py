
import sqlite3
from ma_analyzer import analyze_ma


def main():
    default_path = "twse.db"
    db_path = input(f"請輸入 SQLite 資料庫路徑（預設: {default_path}）：").strip()
    if not db_path:
        db_path = default_path
    stock_id = input("請輸入股票代號，例如 2330：").strip()

    try:
        conn = sqlite3.connect(db_path)
        result = analyze_ma(stock_id, conn)
        print("\n分析結果：")
        print(result.to_string(index=False))
    except Exception as e:
        print(f"發生錯誤：{e}")


if __name__ == "__main__":
    main()
