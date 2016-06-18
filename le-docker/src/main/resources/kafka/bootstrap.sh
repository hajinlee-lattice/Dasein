#!/bin/bash

if [ -z ${KAFKA_NODES} ]; then
    KAFKA_NODES=3
fi

KAFKA=$1      # container name, will be suffixed by 1, 2, 3, ...
ZK_PORT=$2    # zookeeper
BK_PORT=$3    # broker
SR_PORT=$4    # schema-registry  
KR_PORT=$5    # kafka-rest
HA_PORT=$6    # HA proxy

if [ -z ${KAFKA} ]; then
    KAFKA=kafka
fi

if [ -z ${ZK_PORT} ]; then
    ZK_PORT=2080
fi

if [ -z ${BK_PORT} ]; then
    BK_PORT=9092
fi

if [ -z ${SR_PORT} ]; then
    SR_PORT=9022
fi

if [ -z ${KR_PORT} ]; then
    KR_PORT=9023
fi

if [ -z ${HA_PORT} ]; then
    HA_PORT=9025
fi

echo $KAFKA $ZK_PORT $BK_PORT $SR_PORT $KAFKA_NODES

# cleanup
bash ./teardown.sh

# run centos container
echo "provisioning ${KAFKA}1"
docker run -itd -p ${ZK_PORT}:2181 --name ${KAFKA}1 -l cluster.name=${KAFKA} latticeengines/kafka
echo "provisioning ${KAFKA}2"
docker run -itd -p ${BK_PORT}:9092 --name ${KAFKA}2 -l cluster.name=${KAFKA} latticeengines/kafka
echo "provisioning ${KAFKA}3"
docker run -itd --name ${KAFKA}3 -l cluster.name=${KAFKA} latticeengines/kafka
for i in $(seq 4 $KAFKA_NODES);
do
	echo "provisioning ${KAFKA}${i}"
	docker run -itd --name ${KAFKA}$i -l cluster.name=${KAFKA} latticeengines/kafka
done
echo "provisioning haproxy: ${KAFKA}-haproxy"
docker run -itd --name ${KAFKA}-ha -p ${HA_PORT}:80 -p ${SR_PORT}:9022 -p ${KR_PORT}:9023 -l cluster.name=${KAFKA} latticeengines/kafka-haproxy

PROXY_IP=$(docker inspect --format '{{ .NetworkSettings.Networks.kafka.IPAddress }}' ${KAFKA}-ha)
SR_PROXY=http://${PROXY_IP}:9022

TMP_DIR=.${KAFKA}_tmp

rm -rf $TMP_DIR 2> /dev/null || true
mkdir $TMP_DIR
cp zookeeper.properties $TMP_DIR/zookeeper.properties
cp haproxy.cfg $TMP_DIR/haproxy.cfg

ZK_HOSTS=""
BROKERS=""
SR_HOSTS=""
KR_HOSTS=""
for i in $(seq 1 $KAFKA_NODES);
do 
	CONTAINER_NAME=${KAFKA}$i
	docker network connect kafka $CONTAINER_NAME 2> /dev/null

	rm $TMP_DIR/myid 2> /dev/null; touch $TMP_DIR/myid; echo $i >> $TMP_DIR/myid
	docker cp $TMP_DIR/myid $CONTAINER_NAME:/var/lib/zookeeper/myid
	rm $TMP_DIR/myid

	INTER_IP=$(docker inspect --format '{{ .NetworkSettings.Networks.kafka.IPAddress }}' ${CONTAINER_NAME})
	HOST_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${CONTAINER_NAME})
	ZK_HOSTS="${ZK_HOSTS},${INTER_IP}:2181"
	BROKERS="${BROKERS},${HOST_IP}:9092"
	SR_HOSTS="${SR_HOSTS}\n    server ${CONTAINER_NAME} ${INTER_IP}:9022 check"
	KR_HOSTS="${KR_HOSTS}\n    server ${CONTAINER_NAME} ${INTER_IP}:9023 check"

	echo "server.${i}=${INTER_IP}:2888:3888" >> $TMP_DIR/zookeeper.properties
done

ZK_HOSTS=`echo $ZK_HOSTS | cut -d , -f 2-`
BROKERS=`echo $BROKERS | cut -d , -f 2-`

sed -i "s|{{SR_HOSTS}}|$SR_HOSTS|g" $TMP_DIR/haproxy.cfg
sed -i "s|{{KR_HOSTS}}|$KR_HOSTS|g" $TMP_DIR/haproxy.cfg

for i in $(seq 1 $KAFKA_NODES);
do
	for f in server schema-registry kafka-rest control-center;
	do
		rm $TMP_DIR/$f.properties 2> /dev/null
		cp $f.properties $TMP_DIR/$f.properties
	done

	CONTAINER_NAME=${KAFKA}$i	
	INTER_IP=$(docker inspect --format '{{ .NetworkSettings.Networks.kafka.IPAddress }}' ${CONTAINER_NAME})
	HOST_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${CONTAINER_NAME})

	sed -i "s|{{BROKER_ID}}|$i|g" $TMP_DIR/server.properties
	sed -i "s|{{HOST_IP}}|$HOST_IP|g" $TMP_DIR/server.properties
	sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" $TMP_DIR/server.properties

	sed -i "s|{{BROKERS}}|$BROKERS|g" $TMP_DIR/control-center.properties
	sed -i "s|{{CONTROLCENTER_ID}}|$i|g" $TMP_DIR/control-center.properties
	sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" $TMP_DIR/control-center.properties

	sed -i "s|{{INTER_IP}}|$INTER_IP|g" $TMP_DIR/schema-registry.properties
	sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" $TMP_DIR/schema-registry.properties

	sed -i "s|{{BROKER_ID}}|$i|g" $TMP_DIR/kafka-rest.properties
	sed -i "s|{{SR_PROXY}}|$SR_PROXY|g" $TMP_DIR/kafka-rest.properties
	sed -i "s|{{ZK_HOSTS}}|$ZK_HOSTS|g" $TMP_DIR/kafka-rest.properties

	docker cp $TMP_DIR/zookeeper.properties $CONTAINER_NAME:/etc/kafka/zookeeper.properties
	docker cp $TMP_DIR/server.properties $CONTAINER_NAME:/etc/kafka/server.properties
	docker cp $TMP_DIR/control-center.properties $CONTAINER_NAME:/etc/confluent-control-center/control-center.properties
	docker cp $TMP_DIR/schema-registry.properties $CONTAINER_NAME:/etc/schema-registry/schema-registry.properties
	docker cp $TMP_DIR/kafka-rest.properties $CONTAINER_NAME:/etc/kafka-rest/kafka-rest.properties

	rm $TMP_DIR/server.properties
	rm $TMP_DIR/control-center.properties
	rm $TMP_DIR/schema-registry.properties
	rm $TMP_DIR/kafka-rest.properties
done

docker cp ${TMP_DIR}/haproxy.cfg ${KAFKA}-ha:/etc/haproxy/haproxy.cfg
docker network connect kafka ${KAFKA}-ha

rm -rf $TMP_DIR

bash start-kafka.sh ${KAFKA}

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"

echo 'you can use this zk connection to add cluster to kafka-manager: '${ZK_HOSTS}