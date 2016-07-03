#!/usr/bin/env bash

CLUSTER=$1
REDIS_PORT=$2
NETWORK=$3

CLUSTER="${CLUSTER:=redis}"
NETWORK="${NETWORK:=lenet}"
REDIS_PORT="${REDIS_PORT:=6379}"

SERVICE="redis"

source ../functions.sh
verify_consul
teardown_simple_service ${SERVICE} ${CLUSTER}
create_network ${NETWORK}

docker run -d \
    --name ${CLUSTER}_redis \
    -h ${CLUSTER}-redis \
    --net ${NETWORK} \
    -p ${REDIS_PORT}:6379 \
    -l ${SERVICE}.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    redis

register_service ${SERVICE} ${CLUSTER}_redis ${REDIS_PORT}

docker_ps
show_service ${SERVICE}