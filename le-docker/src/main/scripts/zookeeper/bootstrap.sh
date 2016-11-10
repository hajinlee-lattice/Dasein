#!/usr/bin/env bash

CLUSTER=$1
PORT=$2
NETWORK=$3

CLUSTER="${CLUSTER:=zookeeper}"
NETWORK="${NETWORK:=lenet}"
PORT="${PORT:=2181}"

SERVICE="zookeeper"

source ../functions.sh
verify_consul
teardown_simple_service ${SERVICE} ${CLUSTER}
create_network ${NETWORK}

docker run -d \
    --name ${CLUSTER}_zk \
    -h ${CLUSTER}-zk \
    --net ${NETWORK} \
    -e ZK_CLUSTER_SIZE=1 \
    -l ${SERVICE}.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    -p ${PORT}:2181 \
    latticeengines/zookeeper

register_service ${SERVICE} ${CLUSTER}_zk ${PORT}

docker_ps
show_service ${SERVICE}