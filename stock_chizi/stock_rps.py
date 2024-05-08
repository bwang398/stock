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
    and data_day>='2024-01-01' and data_day<='2024-04-25' order by data_day asc 
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
        ,ta.inc_amount_per as inc_amount_per
        ,taa.close_price as  15_close_price
        ,tab.close_price as  25_close_price
        ,tb.close_price  as  40_close_price
        ,tc.close_price  as  60_close_price
        ,td.close_price  as  90_close_price
        ,te.close_price  as  130_close_price
        ,tf.close_price  as  180_close_price
        ,tfa.close_price as  250_close_price
        ,(ta.close_price-taa.close_price)/taa.close_price*100 as 15_ratio 
        ,(ta.close_price-tab.close_price)/tab.close_price*100 as 25_ratio 
        ,(ta.close_price-tb.close_price)/tb.close_price*100   as 40_ratio 
        ,(ta.close_price-tc.close_price)/tc.close_price*100   as 60_ratio 
        ,(ta.close_price-td.close_price)/td.close_price*100   as 90_ratio 
        ,(ta.close_price-te.close_price)/te.close_price*100   as 130_ratio 
        ,(ta.close_price-tf.close_price)/tf.close_price*100   as 180_ratio 
        ,(ta.close_price-tfa.close_price)/tfa.close_price*100 as 250_ratio 
        ,ta.report_time
    from 
    (select  
        ta.stock_code 
        ,ta.stock_name 
        ,ta.close_price
        ,ta.inc_amount_per
        ,ta.report_time 
    from stock_price_info ta
    left join dongcai_stock_info tb on ta.stock_code=tb.security_code
    inner join xueqiu_stock_info tc on ta.stock_code=substr(tc.stock_code,3,6) 
    where ta.report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    and (DATEDIFF(ta.report_time,tb.listing_date)>=300 or tb.listing_date is null)
    and ta.close_price>3.5 and substr(ta.stock_code,1,3)!='688' and substr(ta.stock_code,1,1) not in('8','9')
    )ta 
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info 
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 14,1)
    )taa on ta.stock_code=taa.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 24,1)
    )tab on ta.stock_code=tab.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 39,1)
    )tb on ta.stock_code=tb.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 59,1)
    )tc on ta.stock_code=tc.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price
        ,report_time 
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 89,1)
    )td on ta.stock_code=td.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price 
        ,report_time
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 129,1) 
    )te on ta.stock_code=te.stock_code
    left join 
    (select  
        stock_code 
        ,stock_name 
        ,close_price 
        ,report_time
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 179,1) 
    )tf on ta.stock_code=tf.stock_code
    left join
    (select  
        stock_code 
        ,stock_name 
        ,close_price 
        ,report_time
    from stock_price_info
    where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 249,1) 
    )tfa on ta.stock_code=tfa.stock_code
    where (te.stock_code is not null or tf.stock_code is not null or tfa.stock_code is not null)
    ;

    delete from stock_rps where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1);
    
    replace into stock_rps
    select           
        taa.stock_code 
        ,taa.stock_name 
        ,taa.close_price 
        ,taa.inc_amount_per
        ,taa.15_ratio 
        ,taa.25_ratio 
        ,taa.40_ratio 
        ,taa.60_ratio 
        ,taa.90_ratio 
        ,taa.130_ratio 
        ,taa.180_ratio 
        ,taa.250_ratio 
        ,100-round(taa.15_rank/(te.cnt/100),2)  as 15_score       
        ,100-round(tab.25_rank/(te.cnt/100),2)  as 25_score                 
        ,100-round(ta.40_rank/(te.cnt/100),2)   as 40_score               
        ,100-round(tb.60_rank/(te.cnt/100),2)   as 60_score                      
        ,100-round(tc.90_rank/(te.cnt/100),2)   as 90_score               
        ,100-round(td.130_rank/(te.cnt/100),2)  as 130_score          
        ,100-round(tea.180_rank/(te.cnt/100),2) as 180_score 
        ,100-round(teb.250_rank/(te.cnt/100),2) as 250_score 
        ,taa.report_time
    from 
    (select
        stock_code 
        ,stock_name 
        ,close_price 
        ,inc_amount_per
        ,15_ratio 
        ,25_ratio 
        ,40_ratio 
        ,60_ratio 
        ,90_ratio 
        ,130_ratio 
        ,180_ratio 
        ,250_ratio 
        ,report_time
        ,@curRank15 := @curRank15 + 1 AS 15_rank
    from tmp_stock_rps,
    ( SELECT @curRank15 := 0 ) r ORDER BY 15_ratio  desc
    )taa 
    left join 
    (select
        stock_code
        ,stock_name
        ,25_ratio
        ,report_time
        ,@curRank25 := @curRank25 + 1 AS 25_rank
    from tmp_stock_rps,
    ( SELECT @curRank25 := 0 ) r ORDER BY 25_ratio  desc
    )tab on taa.stock_code=tab.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,40_ratio
        ,report_time
        ,@curRank40 := @curRank40 + 1 AS 40_rank
    from tmp_stock_rps,
    ( SELECT @curRank40 := 0 ) r ORDER BY 40_ratio  desc
    )ta on taa.stock_code=ta.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,60_ratio
        ,report_time
        ,@curRank60 := @curRank60 + 1 AS 60_rank
    from tmp_stock_rps,
    ( SELECT @curRank60 := 0 ) r ORDER BY 60_ratio  desc
    )tb on taa.stock_code=tb.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,90_ratio
        ,report_time
        ,@curRank90 := @curRank90 + 1 AS 90_rank
    from tmp_stock_rps ,
    ( SELECT @curRank90 := 0 ) r ORDER BY 90_ratio  desc
    )tc on taa.stock_code=tc.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,130_ratio
        ,report_time
        ,@curRank130 := @curRank130 + 1 AS 130_rank
    from tmp_stock_rps ,
    ( SELECT @curRank130 := 0 ) r ORDER BY 130_ratio  desc
    )td on taa.stock_code=td.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,180_ratio
        ,report_time
        ,@curRank180 := @curRank180 + 1 AS 180_rank
    from tmp_stock_rps ,
    ( SELECT @curRank180 := 0 ) r ORDER BY 180_ratio  desc
    )tea on taa.stock_code=tea.stock_code
    left join
    (select
        stock_code
        ,stock_name
        ,250_ratio
        ,report_time
        ,@curRank250 := @curRank250 + 1 AS 250_rank
    from tmp_stock_rps ,
    ( SELECT @curRank250 := 0 ) r ORDER BY 250_ratio  desc
    )teb on taa.stock_code=teb.stock_code
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
    # datetime = '2023-12-07'
    # datetime = datetime.datetime.now().strftime('%Y-%m-%d')
    # create_table(datetime)
    print("=======运行RPS成功=========")