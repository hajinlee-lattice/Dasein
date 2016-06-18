#!/bin/bash

if [ -z ${KAFKA_NODES} ]; then
    KAFKA_NODES=3
fi

KAFKA=$1
if [ -z ${KAFKA} ]; then
    KAFKA=kafka
fi

for i in $(seq 1 $KAFKA_NODES);
do 
	docker exec ${KAFKA}$i bash -c 'schema-registry-stop'
done

sleep 2

for i in $(seq 1 $KAFKA_NODES);
do 
	docker exec ${KAFKA}$i bash -c 'kafka-server-stop'	
done

sleep 2

for i in $(seq 1 $KAFKA_NODES);
do 
	docker exec ${KAFKA}$i bash -c 'zookeeper-server-stop'	
done

echo "starting haproxy on ${KAFKA}-ha"
docker exec ${KAFKA}-ha bash /haproxy-shutdown