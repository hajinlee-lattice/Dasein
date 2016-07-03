#!/usr/bin/env bash

CLUSTER=$1
PORT=$2
NETWORK=$3

CLUSTER="${CLUSTER:=consul}"
NETWORK="${NETWORK:=lenet}"
PORT="${PORT:=8500}"

source ../functions.sh

rm_by_label "consul.group" ${CLUSTER}
create_network ${NETWORK}

docker run -d \
    -p ${PORT}:8500 \
    --net ${NETWORK} \
    --name ${CLUSTER}_consul \
    -h ${CLUSTER}-consul \
    -l consul.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    progrium/consul \
    -server -bootstrap -ui-dir /ui

docker_ps