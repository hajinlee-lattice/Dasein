#!/bin/bash

ZK_NODES=$1
ZK_CLUSTER=$2
ZK_PORT=$3
ZK_NETWORK=$4

if [ -z ${ZK_CLUSTER} ]; then
    ZK_CLUSTER=zk
fi

if [ -z ${ZK_NODES} ]; then
    ZK_NODES=3
fi

if [ -z ${ZK_PORT} ]; then
    ZK_PORT=2181
fi

if [ -z ${ZK_NETWORK} ]; then
    ZK_NETWORK=zk
fi

docker network create ${ZK_NETWORK} 2>/dev/null || true

# cleanup
bash ./teardown.sh ${ZK_CLUSTER}

for i in $(seq 1 $ZK_NODES);
do
    if [ $i = 1 ]; then
        if [ $ZK_NODES = 1 ]; then
            echo "Provisioning zookeeper node ${ZK_CLUSTER}-zk"
            docker run -d --name ${ZK_CLUSTER}-zk \
                -h ${ZK_CLUSTER}-zk \
                --net ${ZK_NETWORK} \
                -e ZK_CLUSTER_SIZE=${ZK_NODES} \
                -l cluster.name=${ZK_CLUSTER} \
                -p ${ZK_PORT}:2181 \
                latticeengines/zookeeper
        else
            echo "Provisioning zookeeper node ${ZK_CLUSTER}-zk1"
            docker run -d --name ${ZK_CLUSTER}-zk1 \
                -h ${ZK_CLUSTER}-zk1 \
                --net ${ZK_NETWORK} \
                -e ZK_CLUSTER_SIZE=${ZK_NODES} \
                -e MY_ID=1 \
                -e ZK_CLUSTER_NAME=${ZK_CLUSTER} \
                -l cluster.name=${ZK_CLUSTER} \
                -p ${ZK_PORT}:2181 \
                latticeengines/zookeeper
        fi
    else
        echo "Provisioning zookeeper node ${ZK_CLUSTER}-zk${i}"
        docker run -d --name ${ZK_CLUSTER}-zk${i} \
            -h ${ZK_CLUSTER}-zk${i} \
            --net ${ZK_NETWORK} \
            -e ZK_CLUSTER_SIZE=${ZK_NODES} \
            -e MY_ID=${i} \
            -e ZK_CLUSTER_NAME=${ZK_CLUSTER} \
            -l cluster.name=${ZK_CLUSTER} \
            latticeengines/zookeeper
    fi
done

sleep 1
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"