#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="matchapi"

source ../functions.sh
teardown_simple_service ${SERVICE} ${CLUSTER}

cat ${WSHOME}/le-config/conf/env/dev/latticeengines.properties | head -n 10
rm -rf /tmp/latticeengines.properties
cp ${WSHOME}/le-config/conf/env/dev/latticeengines.properties /tmp/latticeengines.properties
cat /tmp/latticeengines.properties | head -n 10

docker run -d \
    --name ${CLUSTER}_matchapi \
    -h ${CLUSTER}-matchapi \
    --net host \
    -e LE_ENVIRONMENT=dev \
    -e LE_STACK=${LE_STACK} \
    -v /tmp/latticeengines.properties:/etc/ledp/dev/latticeengines.properties \
    -l ${SERVICE}.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    -p 8076:8080 \
    latticeengines/matchapi

docker_ps