#!/bin/bash

# 主机列表
hosts=("hdp01" "hdp02" "hdp03" "hdp04" "hdp05" "hdp06" "hdp07" "hdp08")

# 转发文件
for host in "${hosts[@]}"; do
    echo "copy file to $host"
    scp -r /home/software/zookeeper root@$host:/home/software
done

# 初始化计数器
counter=1

# 循环遍历主机列表
for host in "${hosts[@]}"; do
    echo "Connecting to $host"
    
    # 使用SSH执行命令并输出当前计数器值
    ssh root@$host "mkdir -p /home/software/zookeeper/data/ && echo $counter /home/software/zookeeper/data/myid"
    
    # 递增计数器
    ((counter++))
done