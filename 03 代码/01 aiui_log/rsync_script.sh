#!/bin/bash

# 从远程服务器同步日志
ssh -p 50022 192.168.30.149 "rsync -avz  --progress --port 50783 --password-file ~/passwd hdfs@139.9.45.84::TSPLOG /home/hdfs/data/aiui/platform/log/"