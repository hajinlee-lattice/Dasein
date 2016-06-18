#!/bin/bash

if [ -z ${ZK_NODES} ]; then
    ZK_NODES=3
fi

ZK=$1
if [ -z ${ZK} ]; then
    ZK=zk
fi

for i in $(seq 1 $ZK_NODES);
do
	echo "starting zookeeper on ${ZK}$i"
	docker restart ${ZK}$i
done
