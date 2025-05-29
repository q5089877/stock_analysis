# test_margin.py
# -*- coding:utf-8 -*-

import requests
from bs4 import BeautifulSoup
import random
import os
import sqlite3
import datetime


def head_random():
    hs = [
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
        'Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML, like Gecko) Version/4.0 Mobile/7A341 Safari/528.16',
        'Mozilla/5.0 (Linux; U; Android 4.1.2; zh-tw; GT-I9300 Build/JZO54K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
        'Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)'
    ]
    return {"User-Agent": random.choice(hs)}


def read_network_data(date, from_number=1):
    url = 'http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php'
    head = head_random()
    payload = {"download": '', "qdate": date, "selectType": "ALL"}
    res = requests.post(url, headers=head, data=payload)
    soup = BeautifulSoup(res.text, "lxml")
    tab = soup.select('tbody')[from_number]
    regs = [[] for _ in range(len(tab.select('tr')[0].select('td')))]
    n = 0
    for tr in tab.select('tr'):
        for i in range(len(tab.select('tr')[0].select('td'))):
            if n == 0 or n == 1 or n == 15:
                strs = tr.select('td')[i].text.strip()
                regs[i].append(strs)
            else:
                ints = tr.select('td')[i].text.replace(',', '')
                regs[i].append(ints)
            n += 1
        n = 0
    return regs


def check_db_exists(regs):
    if not os.path.exists('twsedata.db'):
        conn = sqlite3.connect('twsedata.db')
        sql = "CREATE TABLE 股票(股票代號  char(10) PRIMARY KEY,股票名稱  char(20));"
        conn.execute(sql)
        sql = """
            CREATE TABLE  融資融券信用交易統計(
                日期  datetime,
                項目  char(20),
                買進 INTEGER,
                買出 INTEGER,
                現金（券）償還 INTEGER,
                前日餘額 INTEGER,
                今日餘額 INTEGER
            );
        """
        conn.execute(sql)
        conn.commit()
        conn.close()

    conn = sqlite3.connect('twsedata.db')
    for n, i in enumerate(regs[0]):
        try:
            c = conn.cursor()
            sql = f"select * from [{i}]"
            c.execute(sql)
            c.close()
        except Exception:
            c = conn.cursor()
            sql = f"""
                CREATE TABLE [{i}](
                    日期  DATETIME PRIMARY KEY,
                    資買進  INTEGER,
                    資賣出 INTEGER,
                    資現金償還 INTEGER,
                    資前日餘額 INTEGER,
                    資今日餘額 INTEGER,
                    資限額 INTEGER,
                    券買進  INTEGER,
                    券賣出 INTEGER,
                    券現金償還 INTEGER,
                    券前日餘額 INTEGER,
                    券今日餘額 INTEGER,
                    券限額 INTEGER,
                    資券互抵 INTEGER,
                    註記 char(5)
                );
            """
            c.execute(sql)
            sql = f"INSERT OR IGNORE INTO 股票(股票代號, 股票名稱) VALUES (?, ?);"
            c.execute(sql, (regs[0][n], regs[1][n]))
            c.close()
    conn.commit()
    conn.close()


def add_data(regs, date):
    conn = sqlite3.connect('twsedata.db')
    for i in range(len(regs[0])):
        c = conn.cursor()
        sql = f"SELECT * FROM [{regs[0][i]}] WHERE 日期 = ?;"
        c.execute(sql, (date,))
        row = c.fetchone()
        if row:
            c.close()
            continue  # 已有資料，跳過
        sql = f"""
            INSERT INTO [{regs[0][i]}](
                日期, 資買進, 資賣出, 資現金償還, 資前日餘額, 資今日餘額, 資限額,
                券買進, 券賣出, 券現金償還, 券前日餘額, 券今日餘額, 券限額, 資券互抵, 註記
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values = [date] + [regs[j][i] for j in range(2, 17)]
        c.execute(sql, values)
        c.close()
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # 單一測試：抓「2024/05/28」
    test_date = "2024/05/28"
    roc_year = str(int(test_date[:4]) - 1911)
    date_roc = test_date.replace(test_date[:4], roc_year)
    print(f"執行抓取 {date_roc} 的融資融券資料")
    regs = read_network_data(date_roc, 1)
    if regs[0][0] == '查無資料':
        print(f"{date_roc} 無資料")
    else:
        check_db_exists(regs)
        add_data(regs, test_date)
        print(f"{test_date} 寫入資料庫完成！")
