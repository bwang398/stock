#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/7 10:50
# @Author  : Ryu
# @Site    :
# @File    : lrb.py.py
# @Software: PyCharm

import requests
import pymysql
import json
import time


def get_code():
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    cursor = conn.cursor()
    sql = """
    select 
        ta.stock_code
        ,ta.market_type
    from stock_price_info ta 
    WHERE ta.report_time='2024-05-16'
    # where ta.report_time=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    and substr(stock_code,1,1) not in('7','8','9') and substr(stock_code,1,3) not in ('688')
    ;

    """
    cursor.execute(sql)
    return cursor.fetchall()
    conn.commit()

def get_code_bak():
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    cursor = conn.cursor()
    sql = """
    select 
        ta.stock_code
        ,ta.market_type
    from 
    (select 
        ta.stock_code
        ,ta.market_type
    from stock_price_info ta 
    where ta.report_time='2024-05-16'
    # where ta.report_time=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    and substr(stock_code,1,1) not in('7','8','9') and substr(stock_code,1,3) not in ('688')
    )ta
    inner join(
    select 
        ta.stock_code
    from stock_price_info ta 
    group by ta.stock_code having count(1)<401
    )tb on ta.stock_code=tb.stock_code
    ;

    """
    cursor.execute(sql)
    return cursor.fetchall()
    conn.commit()


# 写入数据到mysql
def write_mysql(list):
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    cursor = conn.cursor()
    sql = """
        replace INTO stu.stock_price_info
        (stock_code,stock_name,open_price,high_price,low_price,close_price,inc_amount,inc_amount_per,deal_amount,deal_volume,pe,market_capital,market_type,report_time) 
        VALUES (%s, %s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    cursor.execute(sql, list)
    conn.commit()
    cursor.close()
    conn.close()

# 获取数据
def get_list_url(url):
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/85.0.4183.121 Safari/537.36'}
    print("========正在抓取url数据=========" + url)
    response = requests.get(url, headers=header, timeout=80).text[40:-2]
    json_data = json.loads(response)
    if json_data['data'] is None:
        print(json_data['data'])
    else:
        print(json_data['data'])
        # try:
        stock_code = json_data['data']['code']
        stock_name = json_data['data']['name'].replace(' ', '')
        data_lines_list = json_data['data']['klines']
        # print(data_lines_list)
        for data in data_lines_list:
            data_list = data.split(',')
            print(data_list)
            open_price = data_list[1]
            high_price = data_list[3]
            low_price = data_list[4]
            close_price = data_list[2]
            inc_amount = data_list[9]
            inc_amount_per = data_list[8]
            deal_amount = data_list[5]
            deal_volume = data_list[6]
            pe = None
            market_capital = None
            market_type = None
            report_time = data_list[0]
            #date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            list_d = [stock_code, stock_name, open_price, high_price, low_price, close_price, inc_amount,
                      inc_amount_per, deal_amount, deal_volume, pe, market_capital, market_type, report_time]
            print(list_d)
            if stock_code[:1] == '8' or stock_code[:1] == '7' \
                    or stock_code[:1] == '9' or stock_code[:3] == '688' or stock_code is None:
                pass
            elif stock_name[:2] == 'ST' or stock_name[:1] == '*':
                pass
            elif close_price == '-' or close_price == '_':
                pass
            elif report_time < '2024-05-16':
                print("执行")
                write_mysql(list_d)
            else:
                pass


# 获取url
def get_url():
    result = get_code()
    for i in result:
        # 传入股票代码和名称
        stock_code = i[0]
        market_type = str(i[1])
        url = "http://41.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery3510845991232694919_1678518747390" \
              "&secid={}.{}&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51" \
              "%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end=20500101&lmt=400&_" \
              "=1678518747404".format(market_type, stock_code)
        get_list_url(url)
        time.sleep(0.5)

def get_url_bak():
    result = get_code_bak()
    for i in result:
        # 传入股票代码和名称
        stock_code = i[0]
        market_type = str(i[1])
        url = "http://41.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery3510845991232694919_1678518747390" \
              "&secid={}.{}&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51" \
              "%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end=20500101&lmt=403&_" \
              "=1678518747404".format(market_type, stock_code)
        get_list_url(url)
        time.sleep(0.5)

if __name__ == '__main__':
    # get_url()
    get_url_bak()