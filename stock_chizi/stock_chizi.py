
import pymysql
from pymysql.constants import CLIENT
import datetime
import pandas as pd

# 条件一，rps(rps250+rps130>=170 或者 (25_score>40_score>60_score>90_score and 25_score>85) 
    # 或者(25_score>90 and 40_score>90 and 60_score>90 and 90_score>90 ))
    # 实现3900筛选完500只
def create_rps():
    host = '175.178.92.143'
    user = 'root'
    port = 3306
# client_flag 解决同时执行多条语句的问题
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table if exists tmp_stock_chizi_rps_01;
    create table tmp_stock_chizi_rps_01 as 
    select 
        stock_code
        ,stock_name
        ,close_price
        ,15_score
        ,25_score
        ,40_score
        ,60_score
        ,90_score
        ,130_score
        ,180_score
        ,250_score
        ,case when (
                (130_score+250_score)>=170 
                or (25_score>40_score>60_score>90_score and 25_score>85) 
                or (25_score>90 and 40_score>90 and 60_score>90 and 90_score>90)
                ) then 1 else 0 end as is_rps
        ,report_time
    from(
    select 
        stock_code
        ,stock_name
        ,close_price
        ,15_score
        ,25_score
        ,40_score
        ,60_score
        ,90_score
        ,130_score
        ,180_score
        ,250_score
        ,report_time
        ,row_number() over(partition by stock_code order by report_time desc) as rn
    from stock_rps where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 19,1)
    )ta where ta.rn=1
    ;
    """
    cursor.execute(sql)
    conn.commit()


# 条件二、收盘价 
    # 在过去的30天收盘超过200日均线或者收盘价超过120日均线或者收盘价超过90日均线 的天数大于等于28天
    # 过去5天的10日均线和20日均线保持上升状态
    # 过去5天10日均线>20日均线的天数大于等于1天

def create_avg():
    host = '175.178.92.143'
    user = 'root'
    port = 3306
# client_flag 解决同时执行多条语句的问题
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table  if exists tmp_stock_chizi_avg_01;
    create table tmp_stock_chizi_avg_01 as 
    select 
        stock_code   
        ,stock_name   
        ,sum(close_90)   as close_90_cnt
        ,sum(close_120)  as close_120_cnt
        ,sum(close_200)  as close_200_cnt
    from 
    (select 
        stock_code   
        ,stock_name   
        ,close_price  
        ,10_avg_price
        ,20_avg_price
        ,90_avg_price   
        ,120_avg_price
        ,200_avg_price
        ,case when close_price>90_avg_price  then 1 else 0 end as close_90
        ,case when close_price>120_avg_price then 1 else 0 end as close_120
        ,case when close_price>200_avg_price then 1 else 0 end as close_200
        ,report_time
    from stock_avg_price 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 30,1)
    )ta 
    group by 
        stock_code   
        ,stock_name  
    ;

    drop table  if exists tmp_stock_chizi_avg_02;
    create table tmp_stock_chizi_avg_02 as 
    select 
        stock_code   
        ,stock_name   
        ,sum(ABS(CAST(10_avg_rn as SIGNED)-cast(rn as SIGNED))) as 10_avg
        ,sum(ABS(CAST(20_avg_rn as SIGNED)-cast(rn as SIGNED))) as 20_avg   
    from 
    (select 
        stock_code   
        ,stock_name   
        ,close_price  
        ,10_avg_price
        ,20_avg_price
        ,report_time
        ,row_number() over(partition by stock_code order by report_time desc) as rn 
        ,row_number() over(partition by stock_code order by 10_avg_price desc) as 10_avg_rn 
        ,row_number() over(partition by stock_code order by 20_avg_price desc) as 20_avg_rn 
    from stock_avg_price 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 5,1)
    )ta 
    group by
        stock_code   
        ,stock_name
    ;
    """
    cursor.execute(sql)
    conn.commit()


# 池子
def create_chizi():
    host = '175.178.92.143'
    user = 'root'
    port = 3306
# client_flag 解决同时执行多条语句的问题
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table if exists tmp_stock_chizi;
    create table tmp_stock_chizi as
    select 
        ta.stock_code
        ,ta.stock_name
        ,ta.close_price
        ,ta.is_rps
        ,case when tb.close_90_cnt>=28 or tb.close_120_cnt>=28 or tb.close_200_cnt>=28 then 1
            else 0 end as is_avg
        ,case when tc.10_avg+tc.20_avg<=2 then 1 else 0 end as is_trend
    from tmp_stock_chizi_rps_01 ta 
    left join tmp_stock_chizi_avg_01 tb on ta.stock_code=tb.stock_code
    left join tmp_stock_chizi_avg_02 tc on ta.stock_code=tc.stock_code
    """
    cursor.execute(sql)
    conn.commit()


if __name__ == '__main__':
    # create_rps()
    # create_avg()
    create_chizi()


# 在RPS股价相对强度优先一切的原则下，我最喜欢的第一买点是经历过充分调整之后的第一个启动的口袋支点。
# 如果错过了第一买点，我后面可能会考虑首次下10日线、20日线和50日线的买点，前提是之前的买点必须已经证明是成功的。
# 例如前面的买点买入之后，至少有15%-20%左右的涨幅。


#市场好(必须条件是什么？表象是什么)



#板块的趋势(怎么判断板块趋势好呢),个股rps250的排名


#均价
# 最喜欢的均线顺向火车轨：10日线与20日线的顺向火车轨。
# 这种图形，最常见于行情中的中长线龙头股票。最强势的股票10日线与20日线保持头列排列，一波就完成月线级别的主升浪。
# 最后的主升浪出现之前，10日线会走平或略微向下，向20日线靠拢，10日线再度勾头向上，是绝佳买点。

#rps
# 顺向火车道

# 我考虑观察的对象是RPS250和RPS120这两个。
# 我考虑的条件是RPS250和RPS120二者之和大于185，这其中的含义是RPS250和RPS120二者的最低值都要达到85
# 第二个条件是收盘价站上250日线、200日线和20日线的要求。

# 我要求过去30天内要求收盘价大于250日线的天数大于25天。这个25天是可调参数，如25至30。
# 我要求过去30天内要求收盘价大于200日线的天数大于25天。这个25天是可调参数，如25至30。
# 我要求过去10天内要求收盘价大于20日线的天数大于9天。即过去10天内最多有一天的收盘价低于20日线。


# 第三个条件是要求收盘站上20日线和收盘价是最近一年最高价的80%以上。
# 第四个条件是：10日线一直大于20日线，且20日线一直保持上升状态。
# 要求过去5天的20日线一直保持上升状态。这个5天是可调参数，如5至15。
# 要求过去5天的10日线一直大于20日线。这个5天是可调参数，如5至15。




#股价连续突破新高(最近3个月突破新高的次数(250日))
# 如何来研究中际旭创这个案列