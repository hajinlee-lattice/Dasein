#!/bin/bash

KAFKA=$1            # container name, will be suffixed by 1, 2, 3, ...
KAFKA_NODES=$2
KAFKA_NETWORK=$3    # docker network
ZK_PORT=$4         # zookeeper
BK_PORT=$5          # broker
SR_PORT=$6          # schema-registry  
KR_PORT=$7          # kafka-rest
HA_PORT=$8          # HA proxy

if [ -z ${KAFKA} ]; then
    KAFKA=kafka
fi

if [ -z ${KAFKA_NODES} ]; then
    KAFKA_NODES=2
fi

if [ -z ${KAFKA_NETWORK} ]; then
    KAFKA_NETWORK=kafka
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

# cleanup
bash teardown.sh ${KAFKA}

docker network create ${KAFKA_NETWORK} 2>/dev/null || true

# provision zookeeper
pushd ../zookeeper
bash bootstrap.sh ${KAFKA} ${ZK_PORT} ${KAFKA_NETWORK}
popd

sleep 3

ZK_HOSTS=${KAFKA}-zk1:2181,${KAFKA}-zk2:2181,${KAFKA}-zk3:2181

echo "provisioning ${KAFKA}-bkr1"
docker run -d --name ${KAFKA}-bkr1 -h ${KAFKA}-bkr1 \
    --net ${KAFKA_NETWORK} \
    -e KAFKA_CLUSTER_NAME=${KAFKA} \
    -e DISCOVER_SERVICE=http://${KAFKA}-discover:5000 \
    -l cluster.name=${KAFKA} \
    -p ${BK_PORT}:9092 \
    latticeengines/kafka

for i in $(seq 2 $KAFKA_NODES);
do
    echo "provisioning ${KAFKA}-bkr${i}"
    docker run -d --name ${KAFKA}-bkr${i} -h ${KAFKA}-bkr${i} \
        --net ${KAFKA_NETWORK} \
        -e KAFKA_CLUSTER_NAME=${KAFKA} \
        -e DISCOVER_SERVICE=http://${KAFKA}-discover:5000 \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka
done

sleep 3

for i in $(seq 1 2);
do
    echo "provisioning ${KAFKA}-sr$i"
    docker run -d --name ${KAFKA}-sr${i} -h ${KAFKA}-sr${i}\
        --net ${KAFKA_NETWORK} \
        -e KAFKA_CLUSTER_NAME=${KAFKA} \
        -e DISCOVER_SERVICE=http://${KAFKA}-discover:5000 \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka-schema-registry

    echo "provisioning ${KAFKA}-rest${i}"
    docker run -d --name ${KAFKA}-rest${i} -h ${KAFKA}-rest${i} \
        --net ${KAFKA_NETWORK} \
        -e KAFKA_CLUSTER_NAME=${KAFKA} \
        -e SR_PROXY=http://${KAFKA}-proxy:9022 \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka-rest
done

echo "provisioning haproxy: ${KAFKA}-ha"
docker run -d --name ${KAFKA}-ha \
    --net ${KAFKA_NETWORK} \
    -p ${HA_PORT}:80 -p ${SR_PORT}:9022 -p ${KR_PORT}:9023 \
    -e KAFKA_CLUSTER_NAME=${KAFKA} \
    -l cluster.name=${KAFKA} latticeengines/kafka-haproxy

echo "provisioning kafka-manager: ${KAFKA}-mgr"
docker run -d -p 9000:9000 \
    -e ZK_HOSTS="localhost:2181" \
    --name ${KAFKA}-mgr \
    -h ${KAFKA}-mgr \
    --net ${KAFKA_NETWORK} \
    -l cluster.name=${KAFKA} \
    latticeengines/kafka-manager

sleep 3
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"
echo 'you can use this zk connection to add cluster to kafka-manager: '${ZK_HOSTS}