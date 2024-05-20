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
    and data_day>='2024-04-08' and  data_day<='2024-05-17' order by data_day asc 
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
        ,case when sum_5!=5     then null else sum_5_price*1.0/sum_5     end as 5_avg_price
        ,case when sum_10!=10   then null else sum_10_price*1.0/sum_10   end as 10_avg_price
        ,case when sum_20!=20   then null else sum_20_price*1.0/sum_20   end as 20_avg_price      
        ,case when sum_50!=50   then null else sum_50_price*1.0/sum_50   end as 50_avg_price     
        ,case when sum_120<118  then null else sum_120_price*1.0/sum_120 end as 120_avg_price
        ,case when sum_200<190  then null else sum_200_price*1.0/sum_200 end as 200_avg_price
        ,case when sum_240<230  then null else sum_240_price*1.0/sum_240 end as 240_avg_price
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
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 49,1)
                  then close_price else 0 end 
                 ) as sum_50_price  
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 49,1)
                  then 1 else 0 end 
                 ) as sum_50
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
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 239,1)
                  then close_price else 0 end 
                 ) as sum_240_price
        ,sum(case when report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                  and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 239,1)
                  then 1 else 0 end 
                 ) as sum_240
    from stock_price_info
    where report_time<=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
     and  report_time>=(select data_day from stu.dim_calendar where data_day<=date('{data_time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 199,1)
    group by stock_code
    )aa where sum_120>=118  and sum_5=5 and sum_10=10 and sum_20=20 and sum_50=50
    ;


    replace into stock_avg_price
    select 
        ta.stock_code   
        ,tb.stock_name   
        ,tb.close_price  
        ,ta.5_avg_price
        ,ta.10_avg_price
        ,ta.20_avg_price
        ,ta.50_avg_price                
        ,ta.120_avg_price
        ,ta.200_avg_price
        ,ta.240_avg_price
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
    # datetime = '2024-05-17'
    # # datetime = datetime.datetime.now().strftime('%Y-%m-%d')
    # create_table(datetime)
    # print("=======运行均价成功=========")