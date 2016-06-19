#!/usr/bin/env bash

if [ -z ${KAFKA_NODES} ]; then
    KAFKA_NODES=3
fi

KAFKA=$1
if [ -z ${KAFKA} ]; then
    KAFKA=kafka
fi


bash ../zookeeper/teardown.sh ${KAFKA}

for container in $(docker ps -a --format 'table {{.Names}}' | grep ${KAFKA}-bkr);
do
	echo stopping $container
	docker stop $container
done

for container in $(docker ps -a --format 'table {{.Names}}' | grep ${KAFKA}-sr);
do
	echo stopping $container
	docker stop $container
done

for container in $(docker ps -a --format 'table {{.Names}}' | grep ${KAFKA}-rest);
do
	echo stopping $container
	docker stop $container
done

echo "stopping haproxy ${KAFKA}-ha"
docker stop ${KAFKA}-ha 2> /dev/null || true

docker rm $(docker ps -a -q) 2> /dev/null || true
docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"