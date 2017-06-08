#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="tomcatbase"

source ../functions.sh
# teardown_simple_service ${SERVICE} ${CLUSTER}

#docker run -d \
#    --name ${CLUSTER}_${SERVICE} \
#    --net host \
#    -e LE_ENVIRONMENT=dev \
#    -e LE_STACK=${LE_STACK} \
#    -v ${WSHOME}/le-config/conf/env/dev/latticeengines.properties:/etc/ledp/latticeengines.properties \
#    -l ${SERVICE}.group=${CLUSTER} \
#    -l cluster=${CLUSTER} \
#    -p 80:8080 \
#    -p 443:8443 \
#    latticeengines/${SERVICE}

docker run -d \
    --name ${CLUSTER} \
    -h ${CLUSTER}-tomcat \
    -l ${SERVICE}.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    -p 80:8080 \
    -p 443:8443 \
    -p 1099:1099 \
    -p 10001:10001 \
    -p 10002:10002 \
    latticeengines/tomcatbase

docker_ps