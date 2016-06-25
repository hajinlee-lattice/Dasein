#!/bin/bash

KAFKA_NODES=$1
KAFKA=$2            # cluster name
KAFKA_NETWORK=$3    # docker network
ZK_PORT=$4          # Zookeeper
BK_PORT=$5          # Broker
SR_PORT=$6          # Schema Registry
KR_PORT=$7          # Kafka REST
KC_PORT=$8          # Kafka Connect
HA_PORT=$9          # HA proxy

if [ -z ${KAFKA} ]; then KAFKA=kafka ; fi
if [ -z ${KAFKA_NODES} ]; then KAFKA_NODES=2 ; fi
if [ -z ${KAFKA_NETWORK} ]; then KAFKA_NETWORK=kafka ; fi
if [ -z ${ZK_PORT} ]; then ZK_PORT=2080 ; fi
if [ -z ${BK_PORT} ]; then BK_PORT=9092 ; fi
if [ -z ${SR_PORT} ]; then SR_PORT=9022 ; fi
if [ -z ${KR_PORT} ]; then KR_PORT=9023 ; fi
if [ -z ${KC_PORT} ]; then KC_PORT=9024 ; fi
if [ -z ${HA_PORT} ]; then HA_PORT=9025 ; fi

ZK_NODES=3
ZK_HOSTS=${KAFKA}-zk1:2181,${KAFKA}-zk2:2181,${KAFKA}-zk3:2181
if [ $KAFKA_NODES = 1 ]; then
    ZK_NODES=1
    ZK_HOSTS=${KAFKA}-zk:2181
fi

# cleanup
bash teardown.sh ${KAFKA}

docker network create ${KAFKA_NETWORK} 2>/dev/null || true

# provision zookeeper
pushd ../zookeeper
bash bootstrap.sh ${ZK_NODES} ${KAFKA} ${ZK_PORT} ${KAFKA_NETWORK}
popd
sleep 3

if [ $KAFKA_NODES != 1 ]; then
    echo "Provisioning Kafka Manager: ${KAFKA}-mgr"
    docker run -d -p 9001:9000 \
        --name ${KAFKA}-mgr \
        -h ${KAFKA}-mgr \
        --net ${KAFKA_NETWORK} \
        -e ZK_HOSTS="localhost:2181" \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka-manager

    echo 'wait 10 sec for kafka manager to wake up'
    for i in $(seq 1 10);
    do
        echo $i
        sleep 1
    done
fi

HOSTS=""
BROKER_ADDRS=""
for i in $(seq 1 ${KAFKA_NODES});
do
    if [ $i = 1 ]; then
        if [ $KAFKA_NODES = 1 ]; then
            echo "Provisioning Kafka broker ${KAFKA}-bkr"
            docker run -d --name ${KAFKA}-bkr -h ${KAFKA}-bkr \
                --net ${KAFKA_NETWORK} \
                -e ADVERTISE_IP=${KAFKA}-bkr \
                -e ZK_HOSTS=${ZK_HOSTS} \
                -l cluster.name=${KAFKA} \
                -p ${BK_PORT}:9092 \
                latticeengines/kafka
        else
            echo "Provisioning Kafka broker ${KAFKA}-bkr1"
            docker run -d --name ${KAFKA}-bkr1 -h ${KAFKA}-bkr1 \
                --net ${KAFKA_NETWORK} \
                -e ADVERTISE_IP=${KAFKA}-bkr1 \
                -e ZK_HOSTS=${ZK_HOSTS} \
                -l cluster.name=${KAFKA} \
                -p ${BK_PORT}:9092 \
                latticeengines/kafka

            HOSTS="${HOSTS} ${KAFKA}-bkr1"
            BROKER_ADDRS="${KAFKA}-bkr1:9092"
        fi
    else
        echo "Provisioning Kafka broker ${KAFKA}-bkr${i}"
        docker run -d --name ${KAFKA}-bkr${i} -h ${KAFKA}-bkr${i} \
            --net ${KAFKA_NETWORK} \
            -e ADVERTISE_IP=${KAFKA}-bkr${i} \
            -e ZK_HOSTS=${ZK_HOSTS} \
            -l cluster.name=${KAFKA} \
            latticeengines/kafka

        HOSTS="${HOSTS} ${KAFKA}-bkr${i}"
        BROKER_ADDRS="${BROKER_ADDRS},${KAFKA}-bkr1:9092"
    fi
done

sleep 5

if [ $KAFKA_NODES = 1 ]; then
    echo "Provisioning Schema Registry ${KAFKA}-sr"
    docker run -d --name ${KAFKA}-sr -h ${KAFKA}-sr\
        --net ${KAFKA_NETWORK} \
        -e KAFKA_CLUSTER_SIZE=${KAFKA_NODES} \
        -e ZK_HOSTS=${ZK_HOSTS} \
        -e ADVERTISE_IP=${KAFKA}-bkr \
        -l cluster.name=${KAFKA} \
        -p ${SR_PORT}:9022 \
        latticeengines/schema-registry

    sleep 3

    echo "Provisioning Kafka REST ${KAFKA}-rest"
    docker run -d --name ${KAFKA}-rest -h ${KAFKA}-rest \
        --net ${KAFKA_NETWORK} \
        -e ZK_HOSTS=${ZK_HOSTS} \
        -e SR_PROXY=http://${KAFKA}-sr:9022 \
        -p ${KR_PORT}:9023 \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka-rest

    echo "Provisioning Kafka Connect ${KAFKA}-conn"
    docker run -d --name ${KAFKA}-conn -h ${KAFKA}-conn \
        --net ${KAFKA_NETWORK} \
        -e BOOTSTRAP_SERVERS=kafka-bkr:9092 \
        -e SR_ADDRESS=http://${KAFKA}-sr:9022 \
        -p ${KC_PORT}:9024 \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka-connect

else
    for i in $(seq 1 2);
    do
        echo "Provisioning Schema Registry ${KAFKA}-sr$i"
        docker run -d --name ${KAFKA}-sr${i} -h ${KAFKA}-sr${i}\
            --net ${KAFKA_NETWORK} \
            -e KAFKA_CLUSTER_SIZE=${KAFKA_NODES} \
            -e ZK_HOSTS=${ZK_HOSTS} \
            -e ADVERTISE_IP=${KAFKA}-sr${i} \
            -l cluster.name=${KAFKA} \
            latticeengines/schema-registry

        echo "Provisioning Kafka REST ${KAFKA}-rest${i}"
        docker run -d --name ${KAFKA}-rest${i} -h ${KAFKA}-rest${i} \
            --net ${KAFKA_NETWORK} \
            -e KAFKA_CLUSTER_NAME=${KAFKA} \
            -e ZK_HOSTS=${ZK_HOSTS} \
            -e SR_ADDRESS=http://${KAFKA}-proxy:9022 \
            -l cluster.name=${KAFKA} \
            latticeengines/kafka-rest

         HOSTS="${HOSTS} ${KAFKA}-sr${i} ${KAFKA}-rest${i}"
    done

    sleep 3

    for i in $(seq 1 2);
    do
        echo "Provisioning Kafka Connect ${KAFKA}-conn${i}"
        docker run -d --name ${KAFKA}-conn${i} -h ${KAFKA}-conn${i} \
            --net ${KAFKA_NETWORK} \
            -e BOOTSTRAP_SERVERS="${BROKER_ADDRS}" \
            -e SR_ADDRESS=http://${KAFKA}-proxy:9022 \
            -l cluster.name=${KAFKA} \
            latticeengines/kafka-connect

         HOSTS="${HOSTS} ${KAFKA}-conn${i}"
    done

    sleep 3

    echo "Provisioning HAProxy: ${KAFKA}-ha"
    docker run -d --name ${KAFKA}-ha \
        --net ${KAFKA_NETWORK} \
        -p ${HA_PORT}:80 \
        -p ${SR_PORT}:9022 \
        -p ${KR_PORT}:9023 \
        -p ${KC_PORT}:9024 \
        -e HOSTS="${HOSTS}" \
        -l cluster.name=${KAFKA} \
        latticeengines/kafka-haproxy
fi

sleep 2

docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"

if [ $KAFKA_NODES != 1 ]; then
    echo 'you can use this zk connection to add cluster to kafka-manager: '${ZK_HOSTS}
fi