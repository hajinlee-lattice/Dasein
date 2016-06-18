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
	echo "starting zookeeper on ${KAFKA}$i"
	docker exec ${KAFKA}$i bash -c '/usr/bin/zookeeper-server-start -daemon /etc/kafka/zookeeper.properties'	
done

sleep 2

for i in $(seq 1 $KAFKA_NODES);
do 
	echo "starting kafka on ${KAFKA}$i"
	docker exec ${KAFKA}$i bash -c 'JMX_PORT=9199 /usr/bin/kafka-server-start -daemon /etc/kafka/server.properties'
done

sleep 2

for i in $(seq 1 $KAFKA_NODES);
do 
	echo "starting schema-registry on ${KAFKA}$i"
	docker exec ${KAFKA}$i bash -c 'nohup /usr/bin/schema-registry-start -daemon /etc/schema-registry/schema-registry.properties 2>&1 > /tmp/schema-registry.out &'
done

sleep 2

for i in $(seq 1 $KAFKA_NODES);
do 
	echo "starting kafka-rest on ${KAFKA}$i"
	docker exec ${KAFKA}$i bash -c 'nohup /usr/bin/kafka-rest-start -daemon /etc/kafka-rest/kafka-rest.properties 2>&1 > /tmp/kafka-rest.out &'
done

sleep 2

echo "starting haproxy on ${KAFKA}-haproxy"
docker exec ${KAFKA}-ha bash /haproxy-start

# confluent control center
# docker exec ${KAFKA}1 bash -c 'nohup /usr/bin/control-center-start /etc/confluent-control-center/control-center.properties 2>&1 > /tmp/controlCenter.out &'