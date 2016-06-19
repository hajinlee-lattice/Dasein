#!/usr/bin/env bash

if [ -z ${ZK_NODES} ]; then
    ZK_NODES=3
fi

ZK=$1
if [ -z ${ZK} ]; then
    ZK=zk
fi


# cleanup
for container in $(docker ps -a --format 'table {{.Names}}' | grep ${ZK}-zk);
do
	echo stopping $container
	docker stop $container
done

docker rm $(docker ps -a -q) 2> /dev/null || true

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"