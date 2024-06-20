#!bin/bash

ssh -p 50022 192.168.30.149  "nohup /home/software/filebrowser/filebrowser -p 8068 -a 0.0.0.0 -r /home/robot_bag/ >/root/scripts/logs/filebrowser.log 2>&1 &"
