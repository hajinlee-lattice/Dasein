#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="sqoop"

source ../../functions.sh

docker run -d \
    --name sqoop \
    -p 80:8080 \
    -p 443:8443 \
    latticeengines/sqoop

docker_ps
