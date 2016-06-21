#!/bin/bash

if [ -z ${ZK_NODES} ]; then
    ZK_NODES=3
fi

ZK_CLUSTER=$1
ZK_PORT=$2
ZK_NETWORK=$3

if [ -z ${ZK_CLUSTER} ]; then
    ZK_CLUSTER=zk
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

# run centos container
echo "provisioning ${ZK_CLUSTER}-zk1"
docker run -d --name ${ZK_CLUSTER}-zk1 \
	-h ${ZK_CLUSTER}-zk1 \
    --net ${ZK_NETWORK} \
    -e MY_ID=1 \
	-e ZK_CLUSTER_SIZE=${ZK_NODES} \
	-e ZK_CLUSTER=${ZK_CLUSTER} \
	-l cluster.name=${ZK_CLUSTER} \
	-p ${ZK_PORT}:2181 \
	latticeengines/zookeeper

for i in $(seq 2 $ZK_NODES);
do
	echo "provisioning ${ZK_CLUSTER}-zk${i}"
	docker run -d --name ${ZK_CLUSTER}-zk${i} \
		-h ${ZK_CLUSTER}-zk${i} \
		--net ${ZK_NETWORK} \
		-e MY_ID=${i} \
        -e ZK_CLUSTER_SIZE=${ZK_NODES} \
        -e ZK_CLUSTER=${ZK_CLUSTER} \
		-l cluster.name=${ZK_CLUSTER} \
		latticeengines/zookeeper
done

sleep 1
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Image}}"