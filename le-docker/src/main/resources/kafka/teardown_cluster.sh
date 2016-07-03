#!/usr/bin/env bash

KAFKA=$1
if [ -z ${KAFKA} ]; then
    KAFKA=kafka
fi

for container in $(docker ps --format "table {{.Names}}" --filter=label=cluster.name=${KAFKA});
do
	if [ $container == "NAMES" ]; then continue ; fi && \
	echo stopping $container && \
	docker stop $container &
done

wait

docker rm $(docker ps -a -q) 2> /dev/null || true
docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"