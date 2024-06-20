#!/bin/bash
master_host='hdp01'
user='root'
hosts='hdp01 hdp02 hdp03 hdp04 hdp05 hdp06 hdp07 hdp08'

LOG_DIR='/root/datawarehouse/logs'
export SOFTWARE_HOME=/home/software
JAVA_HOME=/usr/jdk1.8.0_161
export JRE_HOME=/opt/app/jdk1.8.0_161/jre
export HADOOP_HOME=$SOFTWARE_HOME/hadoop/hadoop-3.3.5
export ZK_HOME=$SOFTWARE_HOME/zookeeper/apache-zookeeper-3.5.10-bin
export SPARK_HOME=$SOFTWARE_HOME/spark/spark-3.5.1-bin-hadoop3
export HIVE_HOME=$SOFTWARE_HOME/hive/apache-hive-3.1.2-bin

export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$JRE_HOME/lib:$CLASSPATH
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$ZK_HOME/bin:$JAVA_HOME/bin:$ZK_HOME/bin:$SPARK_HOME/bin:$HIVE_HOME/bin


function start_hadoop()
{
    echo "================     正在启动Hadoop集群              ==========="
    echo "连接到 $master_host"
    ssh $user@$master_host "$HADOOP_HOME/sbin/start-all.sh"
}

function stop_hadoop()
{
    echo "================     正在关闭Hadoop集群              ==========="
    echo "连接到 $master_host"
    ssh $user@$master_host "$HADOOP_HOME/sbin/stop-all.sh"
    sleep 5
}

function start_zookeeper()
{
    echo "================     正在启动Zookeeper              ==========="
    for host in $hosts
    do
        echo "连接到 $host"
        ssh $user@$host "$ZK_HOME/bin/zkServer.sh start"
    done
}

function stop_zookeeper()
{
    echo "================     正在关闭Zookeeper              ==========="
    for host in $hosts
    do
        echo "连接到 $host"
        ssh $user@$host "$ZK_HOME/bin/zkServer.sh stop"
    done
}

function start_spark()
{
    echo "================     正在启动Spark集群               ==========="
    echo "连接到 $master_host"
    ssh $user@$master_host "$SPARK_HOME/sbin/start-all.sh"
}

function stop_spark()
{
    echo "================     正在关闭Spark集群               ==========="
    echo "连接到 $master_host"
    ssh $user@$master_host "$SPARK_HOME/sbin/stop-all.sh"
}
function check_process()
{    
    #检查进程是否运行正常，参数1为进程名，参数2为进程端口
    pid=$(ps -ef 2>/dev/null | grep -v grep | grep -i $1 | awk '{print $2}')
    ppid=$(netstat -nltp 2>/dev/null | grep $2 | awk '{print $7}' | cut -d '/' -f 1)
    echo $pid
    [[ "$pid" =~ "$ppid" ]] && [ "$ppid" ] && return 0 || return 1
}

function start_hive()
{
    echo "================     正在启动Hive metastore服务      ==========="
    metapid=$(check_process HiveMetastore 9083)
    cmd="nohup $HIVE_HOME/bin/hive --service metastore >$LOG_DIR/metastore.log 2>&1 &"
    # cmd=$cmd "sleep 4; hdfs dfsadmin -safemode wait >/dev/null 2>&1"
    [ -z "$metapid" ] && eval $cmd || echo "Metastroe服务已启动"
    echo "================     正在启动Hive hiveserver2服务    ==========="
    server2pid=$(check_process HiveServer2 10000)
    cmd="nohup $HIVE_HOME/bin/hive --service hiveserver2 >$LOG_DIR/hiveServer2.log 2>&1 &"
    [ -z "$server2pid" ] && eval $cmd || echo "HiveServer2服务已启动"
}

function stop_hive()
{   
    echo "================     正在关闭Hive metastore服务      ==========="
    metapid=$(check_process HiveMetastore 9083)
    [ "$metapid" ] && kill $metapid || echo "Metastore服务未启动"
    echo "================     正在关闭Hive hiveserver2服务     ==========="
    server2pid=$(check_process HiveServer2 10000)
    [ "$server2pid" ] && kill $server2pid || echo "HiveServer2服务未启动"

}

function start_jupyter()
{
    echo "================     正在启动jupyter-lab             ==========="
    nohup jupyter-lab --allow-root >$LOG_DIR/jupyterlab.log 2>&1 &
}

function stop_jupyter()
{   
    echo "================     正在关闭jupyter-lab             ==========="
    jupyterpid=$(check_process jupyter 9821)
    [ "$jupyterpid" ] && kill $jupyterpid || echo "jupyter服务未启动"
}

function start_dolphinschedual()
{
    echo "================     正在启动dolphinschedual          ==========="
    bash $SOFTWARE_HOME/dolphinscheduler/apache-dolphinscheduler-3.2.1-bin/bin/start-all.sh
}

function stop_dolphinschedual()
{
    echo "================     正在关闭dolphinschedual          ==========="
    bash $SOFTWARE_HOME/dolphinscheduler/apache-dolphinscheduler-3.2.1-bin/bin/stop-all.sh
}

case $1 in
"start-hive")
    start_hive
    ;;
"start-spark")
    start_spark
    ;;
"start-zookeeper")
    start_zookeeper
    ;;
"start-hadoop")
    start_hadoop
    ;;
"start-jupyter")
    start_jupyter
    ;;
"start-dolphinschedual")
    start_dolphinschedual
    ;;
"start-all")
    echo "================     开始启动所有节点服务            ==========="
    start_zookeeper
    start_hadoop
    start_spark
    start_hive
    start_jupyter
    start_dolphinschedual
    ;;
"stop-hive")
    stop_hive
    ;;
"stop-spark")
    stop_spark
    ;;
"stop-zookeeper")
    stop_zookeeper
    ;;
"stop-hadoop")
    stop_hadoop
    ;;
"stop-jupyter")
    stop_jupyter
    ;;
"stop-dolphinschedual")
    stop_dolphinschedual
    ;;
"stop-all")
    echo "================     开始关闭所有节点服务            ==========="
    stop_spark
    stop_hive
    stop_hadoop
    stop_zookeeper
    stop_jupyter
    stop_dolphinschedual
    ;;
*)
    echo '参数错误！'
    echo 'Usage: '$(basename $0)' start-|stop-(hive|spark|zookeeper|hadoop|jupyter|all)'
    ;;
esac
