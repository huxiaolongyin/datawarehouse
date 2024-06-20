#!/usr/bin/env python
# coding: utf-8

# In[8]:


from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import get_json_object, regexp_extract, col, date_format


# In[2]:


spark = SparkSession.builder\
        .master("spark://hdp01:7077")\
        .config("hive.metastore.uris", "thrift://hdp01:9083")\
        .config("spark.sql.warehouse.dir", "hdfs://htwcluster/warehouse") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .appName("dwd_tts_log") \
        .enableHiveSupport() \
        .getOrCreate()


# In[3]:


# 加载log数据
today = datetime.today().strftime('%Y-%m-%d')
# log_df = spark.read.text("hdfs://htwcluster/warehouse/ods/flume/robot-tts/2024-06-18")
log_df = spark.read.text("hdfs://htwcluster/warehouse/ods/flume/robot-tts/{}".format(today))


# In[10]:


log_df.take(1)


# In[6]:


spark.sql("select * from dwd.dwd_platformlogs_externaltts_forever_inc").printSchema()


# In[13]:


# 数据清洗-非结构化处理
log_df.withColumns({
    "opera_time":col('value').substr(0,12),\
    "json_str":regexp_extract(col('value'),'\{.*\}',0),\
    "vin":get_json_object(col('json_str'), '$.vin'),\
    "content":get_json_object(col('json_str'), '$.content'),\
    "voice_company":get_json_object(col('json_str'), '$.voiceCompany'),\
    "scene":get_json_object(col('json_str'), '$.scene'),\
    "voice_type":get_json_object(col('json_str'), '$.voiceType'),\
    "voice_name":get_json_object(col('json_str'), '$.voiceName'),\
    "engine_type":get_json_object(col('json_str'), '$.engineType'),\
    "create_time":get_json_object(col('json_str'), '$.time'),\
    "create_date":date_format(col('create_time'),'yyyy-MM-dd'),\
}).dropDuplicates().createOrReplaceTempView('df')


# In[16]:


# 数据清洗-null值处理
# spark.sql("select * from df").describe().toPandas().T # 查看数据情况。 数据情况良好，没有空值
pass


# In[20]:


# 数据比较去重写入
spark.sql("select value from dwd.dwd_platformlogs_externaltts_forever_inc").createOrReplaceTempView("df_hive")
sql = """
select 
    df.*
from df
left join df_hive
    on df.value = df_hive.value
where df_hive.value is null
"""
df = spark.sql(sql)


# In[21]:


# 数据写入
df.write.format("hive").partitionBy('create_date').mode("append").saveAsTable("dwd.dwd_platformlogs_externaltts_forever_inc")


# In[17]:


# 创建表结构
sql = """
create table if not exists dwd.dwd_platformlogs_externaltts_forever_inc(
    value string comment '日志原始值' ,
    opera_time string comment '操作时间' ,
    json_str string comment 'json文本' ,
    vin string comment '设备码' ,
    content string comment '语音内容' ,
    voice_company string comment '语音服务方' ,
    scene string comment '场景' ,
    voice_type string comment '语音类型' ,
    voice_name string comment '语音包名' ,
    engine_type string comment '引擎类型' ,
    create_time string comment '创建时间' 
)
comment 'dwd_语音内容'
partitioned by (create_date string)
stored as orc
"""
# spark.sql("drop table if exists dwd.dwd_platformlogs_externaltts_forever_inc")
# spark.sql(sql)

