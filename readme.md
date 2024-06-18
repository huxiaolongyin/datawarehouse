# 背景
本项目主要进行汉特云的数仓建设，涉及到的内容有:
- 大数据环境的部署
- 数据开发规范
- 数仓开发代码
- 运维脚本

# 大数据架构
    主要采用离线 Lambda 架构的方式，这是由Storm的作者Nathan Marz提出，使用流处理技术直接完成那些实时性要求较高的指标计算，然后和离线计算进整合从而给用户 一个完整的实时计算结果

整体架构图如下：

![本地路径](/01%20安装部署/static/架构.png "相对路径演示")
- **存储**：HA HDFS + Hive
- **计算**：Spark
- **调度**：Dolphinschedual
- **分布式协调**：Zookeeper
- **OLAP**：Mysql/doris
- **看板**：Superset
- **数据管理**：Openmetadata

# 各个平台地址

HDFS: 192.168.30.100:9870

SPARK MASTER: 192.168.30.100:7070

Hive：

Dolphinshedual：192.168.30.100:12345/dolphinscheduler/ui

FileBrowser： 192.168.30.149:8068
