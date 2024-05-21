#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/7 15:00
# @Author  : Ryuorder_forward_rpssum_inc
# @Site    :
# @Software: PyCharm


import pymysql
from pymysql.constants import CLIENT
import datetime
import pandas as pd

def get_code():
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    cursor = conn.cursor()
    sql2 = """
    select 
        count(1)
    from stock_rps where report_time='2024-05-20'
    """

    sql3 = """
    SELECT * from tmp_stock_chizi_avg_01 where stock_code='002773'
    """
    sql4 = """
    select 
        stock_code   
        ,stock_name 
        ,report_time
        ,close_price  
        ,10_avg_price
        ,20_avg_price
        ,240_avg_price
        ,case when close_price>240_avg_price   then 1 else 0 end as close_240
        ,case when 10_avg_price>240_avg_price  then 1 else 0 end as avg_10_240
        ,case when 20_avg_price>240_avg_price  then 1 else 0 end as avg_20_240
    from stock_avg_price 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date('2024-05-20') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 29,1)
         and report_time<=(select data_day from stu.dim_calendar where data_day<=date('2024-05-20') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    order by report_time desc limit 10
    """
    sql5 = """
    SELECT count(1) from tmp_stock_chizi_tj order by close_price desc
    """

    cursor.execute(sql5)
    conn.close()
    return cursor.fetchall()


if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    table_name=pd.DataFrame(get_code())
    print(table_name)
