#!/usr/bin/env bash

if [ -z ${ZK_NODES} ]; then
    ZK_NODES=3
fi

ZK=$1
if [ -z ${ZK} ]; then
    ZK=zk
fi

# cleanup
for i in $(seq 1 $ZK_NODES);
do 
    echo "stopping ${ZK}${i}"
    docker stop ${ZK}${i} 2> /dev/null || true
done

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"