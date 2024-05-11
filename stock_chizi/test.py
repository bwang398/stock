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
    sql1 = """
    select stock_name,250_score from stock_rps where report_time='2024-05-08' order by 250_score desc limit 50
    """
    sql2 = """
    select count(1) from tmp_stock_chizi_rps_01 ta 
    left join tmp_stock_chizi_avg_01 tb on ta.stock_code=tb.stock_code 
    where is_rps=1 and (tb.close_90_cnt>=28 or tb.close_120_cnt>=28 or close_200_cnt>=28);
    """
    sql3 = """
    select * from tmp_stock_chizi 
    """


    cursor.execute(sql3)
    conn.close()
    return cursor.fetchall()


if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    table_name=pd.DataFrame(get_code())
    print(table_name)
