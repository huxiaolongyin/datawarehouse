#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pymysql


# In[2]:


spark = SparkSession.builder\
        .master("spark://hdp01:7077")\
        .config("hive.metastore.uris", "thrift://hdp01:9083")\
        .config("spark.sql.warehouse.dir", "hdfs://htwcluster/warehouse") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .appName("ads_aiui_log") \
        .enableHiveSupport() \
        .getOrCreate()


# In[3]:


host = '192.168.30.101'
port = 3306
user = 'root'
password = 'Han2Te0Win-19'
database = 'base'
table='aiui_platform_statistical'


# In[4]:


# 统计指标
sql="""
select 
    vin,
    create_date as statistical_date,
    count(*) as statistical_num,
    '' as remark,
    mark,
    cast(current_timestamp() as string) as correct_time
from dwd.dwd_platformlogs_aiui_forever_inc
group by vin, create_date, mark
"""
spark.sql(sql).createOrReplaceTempView('df_hive')


# In[10]:


# 去重标记
# 读取
url = 'jdbc:mysql://192.168.30.101:3306/base?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=GMT%2B8'
spark.read.jdbc(url, table=table, properties={"user":user, "password":password}).createOrReplaceTempView('df_mysql')

sql = """
select
    t1.*,
    case
        when t1.statistical_num != t2.statistical_num then 'update'
        when t1.statistical_num is not null and t2.statistical_num is null then 'insert'
        else 'no_change'
    end as operation
from df_hive t1
left join df_mysql t2
on t1.vin=t2.vin and t1.statistical_date=t2.statistical_date and t1.mark=t2.mark
"""
spark.sql(sql).createOrReplaceTempView('df_res')


# In[11]:


# 写入数据
df_insert = spark.sql("select * from df_res where operation='insert'")
df_update = spark.sql("select * from df_res where operation='update'")

insert_cols = df_insert.columns[:-1]

# 转化为列表
insert_data = [(row.vin, row.statistical_date, row.statistical_num, row.remark, row.mark, row.correct_time) for row in  df_insert.select(insert_cols).collect()]
update_data = [(row.statistical_num, row.correct_time, row.vin, row.statistical_date, row.mark) for row in df_update.collect()]

# 统计值, 用于后续判断写入情况
insert_count = df_insert.count()
update_count = df_update.count()


# In[8]:


# 写入数据
connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
)
    
insert_query = """
        INSERT INTO {} ({})
        VALUES ({})
        """.format(table, ', '.join(insert_cols), ('%s, '*len(insert_cols))[:-2])

update_query = """
        UPDATE {}
        SET  statistical_num=%s, correct_time=%s
        WHERE vin=%s and statistical_date=%s and mark=%s
        """.format(table)

with connection.cursor() as cursor:
    # 插入操作
    if insert_count == 0:
        pass
    elif insert_count == 1:
        cursor.execute(insert_query, insert_data)
    else:
        cursor.executemany(insert_query, insert_data)

    # 更新操作
    if update_count == 0:
        pass
    elif update_count == 1:
        cursor.execute(update_query, update_data)
    else:
        cursor.executemany(update_query, update_data)
        
# 提交事务
connection.commit()
connection.close()

