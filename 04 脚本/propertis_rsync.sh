#!/bin/bash
user=root
hosts='hdp02 hdp03 hdp04 hdp05 hdp06 hdp07 hdp08'

echo "================     配置同步到本地              ==========="
rsync -av ../01\ 安装部署/conf/hadoop/ $HADOOP_HOME/etc/hadoop/


echo "================     分发环境配置文件              ==========="
for host in $hosts
do 
    rsync -av /etc/profile $user@$host:/etc/profile
    rsync -av ~/.bashrc $user@$host:~/.bashrc
    echo "连接到 $host"
    ssh $user@$host "source /etc/profile && source ~/.bashrc"
done

echo "================     分发脚本             ==========="
for host in $hosts
do 
    rsync -av ~/scripts $user@$host:~/
done

echo "================     分发hadoop配置              ==========="
for host in $hosts
do

    ssh root@hdp01 "rsync -av $HADOOP_HOME/etc/hadoop/ $user@$host:$HADOOP_HOME/etc/hadoop/" 
    
done
