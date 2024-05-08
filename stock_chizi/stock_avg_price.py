#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/7 15:00
# @Author  : Ryuorder_forward_rpssum_inc
# @Site    :
# @Software: PyCharm


import pymysql
from pymysql.constants import CLIENT
import datetime

def get_code():
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    cursor = conn.cursor()
    sql1 = """
    select cast(data_day as char) from stu.dim_calendar 
    where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 
    and data_day>='2024-01-01' and  data_day<='2024-04-25' order by data_day asc 
    """
    cursor.execute(sql1)
    return cursor.fetchall()


def create_table(data_time):
    host = '175.178.92.143'
    user = 'root'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table if exists tmp_stock_avg_1;
    create table tmp_stock_avg_1 as 
    select 
        stock_code
        ,sum_5_price*1.0/sum_5     as 5_avg_price
        ,sum_10_price*1.0/sum_10   as 10_avg_price
        ,sum_20_price*1.0/sum_20   as 20_avg_price
        ,sum_35_price*1.0/sum_35   as 35_avg_price        
        ,sum_60_price*1.0/sum_60   as 60_avg_price
        ,sum_90_price*1.0/sum_90   as 90_avg_price        
        ,sum_120_price*1.0/sum_120 as 120_avg_price
        ,sum_200_price*1.0/sum_200 as 200_avg_price
    from 
    (select 
        stock_code
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 4,1)
                  then close_price else 0 end 
                 ) as sum_5_price 
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 4,1)
                  then 1 else 0 end 
                 ) as sum_5
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
                  then close_price else 0 end 
                 ) as sum_10_price  
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
                  then 1 else 0 end 
                 ) as sum_10
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 19,1)
                  then close_price else 0 end 
                 ) as sum_20_price  
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 19,1)
                  then 1 else 0 end 
                 ) as sum_20 
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 34,1)
                  then close_price else 0 end 
                 ) as sum_35_price  
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 34,1)
                  then 1 else 0 end 
                 ) as sum_35
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 59,1)
                  then close_price else 0 end 
                 ) as sum_60_price  
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 59,1)
                  then 1 else 0 end 
                 ) as sum_60
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 89,1)
                  then close_price else 0 end 
                 ) as sum_90_price 
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 89,1)
                  then 1 else 0 end 
                 ) as sum_90
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 119,1)
                  then close_price else 0 end 
                 ) as sum_120_price
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 119,1)
                  then 1 else 0 end 
                 ) as sum_120
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 199,1)
                  then close_price else 0 end 
                 ) as sum_200_price
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 199,1)
                  then 1 else 0 end 
                 ) as sum_200
    from stock_price_info
    where report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
     and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 199,1)
    group by stock_code
    having count(1)>=180
    )aa where sum_5>0 and sum_10>0 and sum_20>0 and sum_35>0 and sum_60>0 and sum_90>0 and sum_120>0 and sum_200>0
    ;


    replace into stock_avg_price
    select 
        ta.stock_code   
        ,tb.stock_name   
        ,tb.close_price  
        ,ta.5_avg_price
        ,ta.10_avg_price
        ,ta.20_avg_price
        ,ta.35_avg_price   
        ,ta.60_avg_price
        ,ta.90_avg_price   
        ,ta.120_avg_price
        ,ta.200_avg_price
        ,tb.report_time  
    from tmp_stock_avg_1 ta 
    inner join stock_price_info tb on ta.stock_code=tb.stock_code 
        and report_time=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    ;
    """.format(data_time=data_time)
    cursor.execute(sql)
    conn.commit()

if __name__ == '__main__':
    datetimes = get_code()
    for data_time in datetimes:
        print("日期："+data_time[0])
        create_table(data_time[0])
    # data_time = '2023-12-26'
    # data_time = datetime.datetime.now().strftime('%Y-%m-%d')
    # create_table(data_time)
    print("=======运行均价成功=========")