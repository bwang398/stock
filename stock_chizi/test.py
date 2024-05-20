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
        report_time
    from stock_price_info ta where stock_code='603790' 
     and order by report_time desc 
    """

    sql3 = """
    SELECT * from information_schema.`PROCESSLIST` WHERE Time > 1000 AND USER = 'xxx' ORDER BY TIME desc
    """


    cursor.execute(sql3)
    conn.close()
    return cursor.fetchall()


if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    table_name=pd.DataFrame(get_code())
    print(table_name)
