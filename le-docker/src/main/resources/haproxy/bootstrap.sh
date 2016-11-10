#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="haproxy"

source ../functions.sh
# teardown_simple_service ${SERVICE} ${CLUSTER}

docker run -d \
    --name haproxy \
    --net ${NETWORK} \
    -h ${CLUSTER}-matchapi \
    -e LE_ENVIRONMENT=dev \
    -e LE_STACK=${LE_STACK} \
    -e MATCHAPI_HOSTPORT="tomcat:8080" \
    -l ${SERVICE}.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    -p 80:80 \
    latticeengines/haproxy

docker_ps