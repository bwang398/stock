
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
    select cast(data_day as char) from stu.dim_calendar 
    where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 
    and data_day>='2024-01-01' and data_day<='2024-05-16' order by data_day asc 
    """
    cursor.execute(sql1)
    return cursor.fetchall()

# 条件一，rps(rps250+rps120>170 或者 (20_rps+50_rps>180) 
    # 实现4100筛选完650只
def create_rps(datatime):
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
        ,20_score
        ,50_score
        ,120_score
        ,250_score
        ,case when (
                (120_score+250_score)>170 
                or (20_score+50_score)>180
                ) then 1 else 0 end as is_rps
        ,report_time
    from(
    select 
        stock_code
        ,stock_name
        ,close_price
        ,20_score
        ,50_score
        ,120_score
        ,250_score
        ,report_time
        ,row_number() over(partition by stock_code order by report_time desc) as rn
    from stock_rps where report_time=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    )ta where ta.rn=1
    ;
    """.format(time=datatime)
    cursor.execute(sql)
    conn.commit()


# 条件二、收盘价 
    # 均线 近30日10日均线>240日均线 and 近30日20日均线>240日均线  的天数超过25天 
    # and 近30日收盘价超过240日均线 的天数超过25天
    #  筛选后600只
# 条件三
    # 近10日 收盘价>10日线+近10日 收盘价>20日线的天数的天数大于10
# 条件四

# 条件五 (排序)

    

    # 趋势线 10日均线或者20日均线趋势向上


def create_avg(datatime):
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
        ,sum(close_10)   as close_10
        ,sum(close_20)   as close_20
        ,sum(avg_10_20)  as avg_10_20
        ,sum(close_240)  as close_240
        ,sum(avg_10_240) as avg_10_240
        ,sum(avg_20_240) as avg_20_240
    from 
    (select 
        stock_code   
        ,stock_name   
        ,close_price  
        ,10_avg_price
        ,20_avg_price
        ,240_avg_price
        ,case when report_time>=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
                 and report_time<=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                 and close_price>10_avg_price then 1 else 0 end as close_10
        ,case when report_time>=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
                 and report_time<=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                 and close_price>20_avg_price then 1 else 0 end as close_20
        ,case when report_time>=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
                 and report_time<=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
                 and 10_avg_price>20_avg_price then 1 else 0 end as avg_10_20
        ,case when close_price>240_avg_price   then 1 else 0 end as close_240
        ,case when 10_avg_price>240_avg_price  then 1 else 0 end as avg_10_240
        ,case when 20_avg_price>240_avg_price  then 1 else 0 end as avg_20_240
        ,report_time
    from stock_avg_price 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 29,1)
         and report_time<=(select data_day from stu.dim_calendar where data_day<=date('{time}') and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    )ta group by stock_code
    ;
    """.format(time=datatime)
    cursor.execute(sql)
    conn.commit()


# 池子
def create_jbm():
    host = '175.178.92.143'
    user = 'root'
    port = 3306
# client_flag 解决同时执行多条语句的问题
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table if exists tmp_stock_chizi_jbm_1;
    create table tmp_stock_chizi_jbm_1 as 
    select 
      security_code
      ,security_name
      ,total_operate_income
      ,ystz
      ,parent_netprofit
      ,sjltz
      ,weightavg_roe
      ,report_date
      ,row_number() over(partition by security_code order by report_date desc) as rn 
      ,row_number() over(partition by security_code,substr(report_date,6,5) order by report_date desc) as rm 
    from dongcai_yjbb where substr(report_date,1,4)>='2019'
    ;
    """
    cursor.execute(sql)
    conn.commit()


# 池子
# 一、满足条件一且满足条件二的有390

def create_chizi():
    host = '175.178.92.143'
    user = 'root'
    port = 3306
# client_flag 解决同时执行多条语句的问题
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    drop table if exists tmp_stock_chizi_tj;
    create table tmp_stock_chizi_tj as
    select 
        ta.stock_code
        ,ta.stock_name
        ,ta.close_price
        ,ta.report_time
    from tmp_stock_chizi_rps_01 ta 
    left join tmp_stock_chizi_avg_01 tb on ta.stock_code=tb.stock_code
    where ta.is_rps=1 and (tb.close_240>=25 and tb.avg_10_240>=25 and tb.avg_20_240>=25)
        and (close_10+close_20>10 and tb.avg_10_20>=3)
    ;

    # drop table if exists tmp_stock_chizi;
    # create table tmp_stock_chizi as 
    # select 
    #     ta.stock_code  
    #     ,taa.stock_code as xueqiu_stock_code
    #     ,ta.stock_name   
    #     ,ta.close_price
    #     ,ta.pe
    #     ,ta.inc_amount_per
    #     ,ta.market_capital
    #     ,tb.main_business
    #     ,tb.businessProducts as main_guanzhu
    #     ,ta.report_time
    # from 
    # (select 
    #     stock_code   
    #     ,stock_name   
    #     ,close_price
    #     ,pe
    #     ,inc_amount_per
    #     ,market_capital
    #     ,report_time
    # from tmp_stock_chizi_close_01
    # where rm=1
    # )ta 
    # left join dongcai_stock_info tb on ta.stock_code=tb.security_code
    # inner join tmp_stock_chizi_tj tc on ta.stock_code=tc.stock_code
    # left join xueqiu_stock_info taa on ta.stock_code=substr(taa.stock_code,3,6)
    # left join (
    # select 
    #     stock_code
    #     ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 4,1)
    #       and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 1,1)
    #       then 1 else 0 end) as sum_5_cnt
    #     ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
    #       and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 1,1)
    #       then 1 else 0 end) as sum_10_cnt
    #     ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 19,1)
    #       and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 1,1)
    #       then 1 else 0 end) as sum_20_cnt
    #     ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 39,1)
    #       and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 1,1)
    #       then 1 else 0 end) as sum_40_cnt
    # from stock_chizi
    # where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 39,1)
    #       and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 1,1)
    # group by stock_code
    # )td on ta.stock_code=td.stock_code
    # where tc.is_rank>0
    # ;

    # replace stock_chizi 
    # select 
    #     stock_code  
    #     ,stock_name   
    #     ,close_price
    #     ,pe
    #     ,inc_amount_per
    #     ,market_capital
    #     ,report_time
    #     ,is_rank
    # from tmp_stock_chizi;
    """
    cursor.execute(sql)
    conn.commit()

# 条件三
# 首次进入池子到目前<=8天,累计出现次数大于等于5天

if __name__ == '__main__':
    # datetimes=get_code()
    # for datetime in datetimes:
    #     print("日期："+str(datetime[0]))
    #     create_rps(datetime[0])
    datetime = '2024-05-20'
    create_rps(datetime)
    create_avg(datetime)
    # create_jbm()
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