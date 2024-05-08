
import pymysql
from pymysql.constants import CLIENT
import datetime
import pandas as pd

# 条件一，rps(rps250+rps130>=170 或者 (25_score>40_score>60_score>90_score and 25_score>85) 
# 或者(25_score>90 and 40_score>90 and 60_score>90 and 90_score>90 ))
# 实现3900筛选完500只

def get_code():
    host = '175.178.92.143'
    user = 'root'
    database = 'stu'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', database=database, port=port, charset="utf8")
    cursor = conn.cursor()
    sql = """
    select report_time,count(1) from stock_rps where report_time>='2024-01-06' and ((130_score+250_score)>=170 
    or (25_score>40_score>60_score>90_score and 25_score>85) 
    or (25_score>90 and 40_score>90 and 60_score>90 and 90_score>90))
    group by report_time order by report_time desc ;
    """
    sql1 = """
    select report_time,count(1) from stock_avg_price where report_time>='2024-01-06' 
    group by report_time
    """
    cursor.execute(sql1)
    return pd.DataFrame(cursor.fetchall())


def create_table1(data_time):
    host = 'localhost'
    user = 'root'
    port = 3306
    conn = pymysql.connect(host=host, user=user, password='141812wa', port=port,
                           charset="utf8", database="stu", client_flag=CLIENT.MULTI_STATEMENTS)
    cursor = conn.cursor()
    sql = """
    ;

    """.format(data_time=data_time)
    cursor.execute(sql)
    conn.commit()

if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    table_name=get_code()
    print(table_name)



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