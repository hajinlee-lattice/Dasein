#!/usr/bin/env bash

ZK=$1
if [ -z ${ZK} ]; then
    ZK=zk
fi

# cleanup
for container in $(docker ps --format "table {{.Names}}" --filter=label=cluster.name=${ZK});
do
	if [ $container == "NAMES" ]; then continue ; fi && \
	echo stopping $container && \
	docker stop $container &
done
wait

docker rm $(docker ps -a -q) 2> /dev/null || true

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"