#! /bin/sh
while [ 1 -eq 1 ]
do
    df -h|grep "/data"|awk '{print strftime("%d-%m-%Y %H:%M:%S",systime()), $0}' >> /data/logs/watchdata.txt
    sleep 60
done