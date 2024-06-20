#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
        .appName("dwd_aiui_log") \
        .enableHiveSupport() \
        .getOrCreate()


# In[4]:


# 加载log数据
today = datetime.today().strftime('%Y-%m-%d')
# log_df = spark.read.text("hdfs://htwcluster/warehouse/ods/flume/aiui-reprocessing/2024-06-18")
log_df = spark.read.text("hdfs://htwcluster/warehouse/ods/flume/aiui-reprocessing/{}".format(today))


# In[5]:


# 数据清洗-非结构化处理
log_df.withColumns({"opera_time":col('value').substr(0,12),\
                    "json_str":regexp_extract(col('value'),'\{.*\}',0),\
                    "vin":get_json_object(col('json_str'), '$.vin'),\
                    "create_time":get_json_object(col('json_str'), '$.createTime'),\
                    "msg_type":get_json_object(col('json_str'), '$.Msg.Type'),\
                    "service_type":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.serviceType'),\
                    "msg_text":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.text'),\
                    "rc":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.rc'),\
                    "topic_id":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.answer.topicID'),\
                    "emotion":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.answer.emotion'),\
                    "question_text":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.answer.question.question'),\
                    "answer_type":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.answer.answerType'),\
                    "answer_text":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.answer.text'),\
                    "msg_uuid":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.uuid'),\
                    "msg_sid":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.sid'),\
                    "service_name":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.serviceName'),\
                    "operation":get_json_object(col('json_str'), '$.Msg.ContentJson.intent.operation'),\
                    "mark":get_json_object(col('json_str'), '$.mark'),\
                    "create_date":date_format(col('create_time'),'yyyy-MM-dd'),\
                   }).dropDuplicates().createOrReplaceTempView('df')


# In[34]:


# 数据清洗-null值处理
# spark.sql("select * from df").describe().toPandas().T # 查看数据情况
sql = """
select
    value,
    opera_time,
    json_str,
    coalesce(vin,'-99') as vin,
    create_time,
    msg_type,
    service_type,
    msg_text, 
    coalesce(rc,'-99') as rc,
    if(topic_id=='null',null,topic_id) as topic_id,
    emotion,
    question_text,
    answer_type,
    answer_text,
    msg_uuid,	
    msg_sid,
    service_name,
    operation,
    coalesce(mark,'1') as mark,
    create_date
from df
"""
spark.sql(sql).createOrReplaceTempView('df')


# In[35]:


# 数据比较去重写入
spark.sql("select value from dwd.dwd_platformlogs_aiui_forever_inc").createOrReplaceTempView('df_hive')
sql = """
select 
    df.*
from df
left join df_hive
    on df.value = df_hive.value
where df_hive.value is null
"""
df = spark.sql(sql)


# In[37]:


# 数据写入
df.write.format("hive").partitionBy('create_date').mode("append").saveAsTable("dwd.dwd_platformlogs_aiui_forever_inc")


# In[36]:


# 创建表结构
sql = """
create table if not exists dwd.dwd_platformlogs_aiui_forever_inc(
    value string comment '日志原始值' ,
    opera_time string comment '操作时间' ,
    json_str string comment 'json文本' ,
    vin string comment '设备码' ,
    create_time string comment '创建时间' ,
    msg_type string comment '消息类型' ,
    service_type string comment '服务类型' ,
    msg_text string comment '消息内容' ,
    rc string comment '' ,
    topic_id string comment '话题id' ,
    emotion string comment '表情' ,
    question_text string comment '问题内容' ,
    answer_type string comment '回答类型' ,
    answer_text string comment '问题内容' ,
    msg_uuid string comment '消息uuid' ,
    msg_sid string comment '消息sid' ,
    service_name string comment '服务商名称' ,
    operation string comment '操作类型' ,
    mark string comment '是否汉特云命中' 
)
comment 'dwd_ai服务接口调用日志明细表'
partitioned by (create_date string)
stored as orc
"""
# spark.sql("drop table if exists dwd.dwd_platformlogs_aiui_forever_inc")
# spark.sql(sql)

