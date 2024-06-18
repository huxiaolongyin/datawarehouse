#!/bin/bash
# EPEL存储库提供了一些在默认存储库中不可用的附加软件包
yum install epel-release -y
# yum-utils软件包提供了一组工具和插件，用于管理和扩展yum软件包管理器的功能
yum install -y yum-utils
# SCL提供了一种在单个系统上同时安装和使用多个软件包版本的方法
yum install centos-release-scl -y
# 提供了在CentOS系统上与软件集合（SCL）一起工作的实用程序和脚本
yum install scl-utils -y
# SCL提供了一种在单个系统上同时安装和使用多个软件包版本的方法
yum install https://mirrors.aliyun.com/ius/ius-release-el7.rpm -y
# rh-python38是一个软件集合，其中包含Python 3.8和相关的软件包和库
yum install -y rh-python38 which

ln -s /opt/rh/rh-python38/root/usr/bin/python3.8 /usr/bin/python3
ln -s /opt/rh/rh-python38/root/usr/bin/pip3.8 /usr/bin/pip3
