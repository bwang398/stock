#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/7 15:00
# @Author  : Ryu
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
    and data_day>='2024-05-17' and data_day<='2024-05-20' order by data_day asc 
    """
    cursor.execute(sql1)
    return cursor.fetchall()


def create_table(datatime):
    host = '175.178.92.143'
    user = 'root'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table if exists stu.tmp_stock_rps
    ;

    create table stu.tmp_stock_rps as 
    select  
        ta.stock_code 
        ,ta.stock_name 
        ,ta.close_price    as  close_price 
        ,(ta.close_price-taa.close_price)/taa.close_price*100 as 5_ratio 
        ,(ta.close_price-tab.close_price)/tab.close_price*100 as 10_ratio 
        ,(ta.close_price-tb.close_price)/tb.close_price*100   as 20_ratio 
        ,(ta.close_price-tc.close_price)/tc.close_price*100   as 50_ratio 
        ,(ta.close_price-td.close_price)/td.close_price*100   as 120_ratio 
        ,(ta.close_price-te.close_price)/te.close_price*100   as 250_ratio 
        ,ta.report_time
    from 
    (select  
        ta.stock_code 
        ,ta.stock_name 
        ,ta.close_price
        ,ta.report_time 
    from stock_price_info ta
    left join dongcai_stock_info tb on ta.stock_code=tb.security_code
    where ta.report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    and (DATEDIFF(ta.report_time,tb.listing_date)>=120 or tb.listing_date is null)
    and substr(ta.stock_code,1,3)!='688' and substr(ta.stock_code,1,1) not in('8','9')
    )ta 
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info 
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 4,1)
    )taa on ta.stock_code=taa.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
    )tab on ta.stock_code=tab.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 19,1)
    )tb on ta.stock_code=tb.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 49,1)
    )tc on ta.stock_code=tc.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 119,1)
    )td on ta.stock_code=td.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price 
        ,report_time
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 249,1) 
    )te on ta.stock_code=te.stock_code
    where taa.stock_code is not null and tab.stock_code is not null and tb.stock_code is not null and tc.stock_code is not null
         and td.stock_code is not null and te.stock_code is not null
    ;

    delete from stock_rps where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1);

    replace into stock_rps
    select           
        taa.stock_code 
        ,taa.stock_name 
        ,taa.close_price 
        ,taa.5_ratio 
        ,taa.10_ratio 
        ,taa.20_ratio 
        ,taa.50_ratio 
        ,taa.120_ratio 
        ,taa.250_ratio 
        ,100-round(taa.5_rank/(te.cnt/100),2)   as  5_score       
        ,100-round(tab.10_rank/(te.cnt/100),2)  as 10_score                 
        ,100-round(ta.20_rank/(te.cnt/100),2)   as 20_score               
        ,100-round(tb.50_rank/(te.cnt/100),2)   as 50_score   
        ,100-round(tc.120_rank/(te.cnt/100),2)  as 120_score               
        ,100-round(td.250_rank/(te.cnt/100),2)  as 250_score          
        ,taa.report_time
    from 
    (select
        stock_code 
        ,stock_name 
        ,close_price 
        ,5_ratio 
        ,10_ratio 
        ,20_ratio 
        ,50_ratio 
        ,120_ratio 
        ,250_ratio
        ,report_time
        ,row_number() over(partition by report_time order by 5_ratio desc) as 5_rank
    from tmp_stock_rps
    )taa 
    left join 
    (select
        stock_code
        ,stock_name
        ,10_ratio 
        ,report_time
        ,row_number() over(partition by report_time order by 10_ratio desc) as 10_rank
    from tmp_stock_rps
    )tab on taa.stock_code=tab.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,20_ratio 
        ,report_time
        ,row_number() over(partition by report_time order by 20_ratio desc) as 20_rank
    from tmp_stock_rps
    )ta on taa.stock_code=ta.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,50_ratio 
        ,report_time
        ,row_number() over(partition by report_time order by 50_ratio desc) as 50_rank
    from tmp_stock_rps
    )tb on taa.stock_code=tb.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,120_ratio 
        ,report_time
        ,row_number() over(partition by report_time order by 120_ratio desc) as 120_rank
    from tmp_stock_rps
    )tc on taa.stock_code=tc.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,250_ratio 
        ,report_time
        ,row_number() over(partition by report_time order by 250_ratio desc) as 250_rank
    from tmp_stock_rps
    )td on taa.stock_code=td.stock_code
    left join(
    select 
     count(1) as cnt
    from tmp_stock_rps
    )te on 1=1
    ;
    """.format(time=datatime)
    cursor.execute(sql)
    conn.commit()


if __name__ == '__main__':
    datetimes=get_code()
    for datetime in datetimes:
        print("日期："+str(datetime[0]))
        create_table(datetime[0])
    # datetime = '2024-05-17'
    # datetime = datetime.datetime.now().strftime('%Y-%m-%d')
    # create_table(datetime)
    print("=======运行RPS成功=========")