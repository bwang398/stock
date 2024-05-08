#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/7 15:00
# @Author  : Ryu
# @Site    :
# @Software: PyCharm

import pymysql
from pymysql.constants import CLIENT
import datetime
import pandas as pd
import matplotlib.pyplot as plt


def get_code():
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    connection = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    sql2="""
    select stock_code,stock_name from stock_rps where report_time='2024-04-26' order by 40_score desc limit 20
    """
   
    sql1 = """
    select stock_code,stock_name,close_price,15_score,25_score,40_score,60_score,90_score,130_score,250_score,report_time from stock_rps where stock_name='中信海直' order by report_time desc;
    """
    # df1 = pd.read_sql(sql2, connection)
    # print(df1)

    df = pd.read_sql(sql1, connection)
    print(df)

    df['date'] = pd.to_datetime(df['report_time'])
    # 设置date列为索引
    df.set_index('date', inplace=True)
    
    # 绘制折线图
    df.plot(kind='line')
    plt.show()

    connection.close()



if __name__ == '__main__':
    # 设置打印选项，以便打印出更多的数据
    pd.set_option('display.max_rows', None)  # 设置打印最大行数为无限
    pd.set_option('display.max_columns', None)  # 设置打印最大列数为无限
    pd.set_option('display.width', None)  # 自动检测控制台的宽度
    pd.set_option('display.max_colwidth', None)  # 设置列的最大宽度为
    get_code()
