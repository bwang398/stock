
import pymysql
from pymysql.constants import CLIENT
import datetime
import pandas as pd

# 条件一，rps(rps250+rps130>=170 或者 (25_score>40_score>60_score>90_score and 25_score>90) 
    # 或者(25_score>90 and 40_score>90 and 60_score>90 and 90_score>90))
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
                or (25_score>40_score>60_score>90_score and 25_score>90 and 40_score>80) 
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
    # 过去7天收盘价>20日均线的天数大于等于4天


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
        ,sum(close_90)   as close_90_cnt
        ,sum(close_120)  as close_120_cnt
        ,sum(close_200)  as close_200_cnt
        ,sum(close_20)   as close_20_cnt
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
<<<<<<< HEAD
        ,case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 6,1) 
              and close_price>20_avg_price then 1 else 0 end as close_20
=======
        ,case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 7,1)
              and  close_price>20_avg_price then 1 else 0 end as close_20
>>>>>>> 8e88ae7d175ea6d5561703ae7b793004242987bf
        ,report_time
    from stock_avg_price 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 30,1)
         and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    )ta 
    group by 
        stock_code   
    ;

    drop table  if exists tmp_stock_chizi_close_01;
    create table tmp_stock_chizi_close_01 as 
    select 
        stock_code   
        ,stock_name   
        ,close_price
        ,pe
        ,inc_amount_per
        ,round(market_capital/100000000,0) as market_capital
        ,rn
        ,rm
        ,report_time
    from 
    (select 
        stock_code   
        ,stock_name   
        ,close_price
        ,pe
        ,inc_amount_per
        ,market_capital
        ,row_number() over(partition by stock_code order by close_price desc) as rn
        ,row_number() over(partition by stock_code order by report_time desc) as rm
        ,report_time
    from stock_price_info 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 90,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    )ta 
    ;

    drop table  if exists tmp_stock_chizi_close_02;
    create table tmp_stock_chizi_close_02 as 
    select 
        stock_code   
        ,stock_name   
        ,close_price
        ,rn
        ,rm
        ,report_time
    from 
    (select 
        stock_code   
        ,stock_name   
        ,close_price
        ,row_number() over(partition by stock_code order by close_price desc) as rn
        ,row_number() over(partition by stock_code order by report_time desc) as rm
        ,report_time
    from stock_price_info 
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 250,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
    )ta 
    ;

    drop table  if exists tmp_stock_chizi_close_03;
    create table tmp_stock_chizi_close_03 as 
    select 
        ta.stock_code
        ,case when ta.max_cnt>=15 then 4
              when ta.max_10_cnt>=7  then 3
              when ta.max_10_cnt>=5  then 2
              when ta.max_1_cnt=1    then 1
              else 0 end as is_90_rank
        ,case when tb.max_cnt>=15 then 4
              when tb.max_10_cnt>=7  then 3
              when tb.max_10_cnt>=5  then 2
              when tb.max_1_cnt=1    then 1
              else 0 end as is_250_rank
    from 
    (select 
        stock_code
        ,sum(case when rn<=20 then 1 else 0 end) as max_cnt
        ,sum(case when rm<=10 and rn<=10 then 1 else 0 end) as max_10_cnt 
        ,sum(case when rn=1 then 1 else 0 end) as max_1_cnt
    from tmp_stock_chizi_close_01 where rm<=20
    group by stock_code
    )ta 
    left join
    (select 
        stock_code
        ,sum(case when rn<=20 then 1 else 0 end) as max_cnt
        ,sum(case when rm<=10 and rn<=10 then 1 else 0 end) as max_10_cnt 
        ,sum(case when rn=1 then 1 else 0 end) as max_1_cnt
    from tmp_stock_chizi_close_02 where rm<=20
    group by stock_code
    )tb on ta.stock_code=tb.stock_code
    ;
    """
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
        ,case when tc.is_250_rank>0 then is_90_rank+is_250_rank+4
              else tc.is_90_rank end as is_rank
    from tmp_stock_chizi_rps_01 ta 
    left join tmp_stock_chizi_avg_01 tb on ta.stock_code=tb.stock_code
    left join tmp_stock_chizi_close_03 tc on ta.stock_code=tc.stock_code
    where ta.is_rps=1 and (tb.close_90_cnt>=28 or tb.close_120_cnt>=28 or tb.close_200_cnt>=28) 
        and close_20_cnt>=4
    ;

    drop table if exists tmp_stock_chizi;
    create table tmp_stock_chizi as 
    select 
        ta.stock_code  
        ,taa.stock_code as xueqiu_stock_code
        ,ta.stock_name   
        ,ta.close_price
        ,ta.pe
        ,ta.inc_amount_per
        ,ta.market_capital
        ,tb.main_business
        ,tb.businessProducts as main_guanzhu
        ,ta.report_time
        ,row_number() over(partition by 1 order by 
            case when td.sum_5_cnt>=3 and td.sum_5_cnt=td.sum_10_cnt then 4
                 when td.sum_10_cnt=td.sum_20_cnt and td.sum_10_cnt>=6 then 3
                 when td.sum_10_cnt>=6 then 2
                 when td.sum_40_cnt>=20 then 1
                 else 0 end desc
            ,tc.is_rank desc
            ,ta.close_price desc
        ) as is_rank
    from 
    (select 
        stock_code   
        ,stock_name   
        ,close_price
        ,pe
        ,inc_amount_per
        ,market_capital
        ,report_time
    from tmp_stock_chizi_close_01
    where rm=1
    )ta 
    left join dongcai_stock_info tb on ta.stock_code=tb.security_code
    inner join tmp_stock_chizi_tj tc on ta.stock_code=tc.stock_code
    left join xueqiu_stock_info taa on ta.stock_code=substr(taa.stock_code,3,6)
    left join (
    select 
        stock_code
        ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 4,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
          then 1 else 0 end) as sum_5_cnt
        ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 9,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
          then 1 else 0 end) as sum_10_cnt
        ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 19,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
          then 1 else 0 end) as sum_20_cnt
        ,sum(case when report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 39,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 0,1)
          then 1 else 0 end) as sum_40_cnt
    from stock_chizi
    where report_time>=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 40,1)
          and report_time<=(select data_day from stu.dim_calendar where data_day<=date(now()) and is_weekend=0 and is_holiday=0 and is_week=1 order by data_day desc limit 1,1)
    group by stock_code
    )td on ta.stock_code=td.stock_code
    where tc.is_rank>0
    ;

    replace stock_chizi 
    select 
        stock_code  
        ,stock_name   
        ,close_price
        ,pe
        ,inc_amount_per
        ,market_capital
        ,report_time
        ,is_rank
    from tmp_stock_chizi;
    """
    cursor.execute(sql)
    conn.commit()

# 条件三
# 首次进入池子到目前<=8天,累计出现次数大于等于5天

if __name__ == '__main__':
    create_rps()
<<<<<<< HEAD
    create_avg()
    create_jbm()
    create_chizi()
=======
    # create_avg()
    # create_chizi()
>>>>>>> 8e88ae7d175ea6d5561703ae7b793004242987bf


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