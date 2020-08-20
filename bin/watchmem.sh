#! /bin/sh
while [ 1 -eq 1 ]
do
    ps -o rss  -C 'cas' | tail -n +2 | paste -s -d+ - | bc |awk '{print strftime("%d-%m-%Y %H:%M:%S",systime()), $0}' >> /data/logs/watchmem.txt
    sleep 10
done