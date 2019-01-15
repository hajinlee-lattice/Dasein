#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="lp"

source ../functions.sh
# teardown_simple_service ${SERVICE} ${CLUSTER}

#docker run -d --net host \
#    --name ${CLUSTER}_${SERVICE} \
#    -h ${HOSTNAME} \
#    -e LE_ENVIRONMENT=dev \
#    -e LE_STACK=${LE_STACK} \
#    -e ENABLE_JACOCO=false \
#    -e CATALINA_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m" \
#    -v ${WSHOME}/le-config/conf/env/dev/latticeengines.properties:/etc/ledp/latticeengines.properties \
#    -v ${WSHOME}/le-docker/src/main/scripts/tomcat/jacoco:/mnt/efs/jacoco:rw \
#    -v ${WSHOME}/le-docker/src/main/scripts/tomcat/logs:/opt/apache-tomcat-8.5.15/logs:rw \
#    -l ${SERVICE}.group=${CLUSTER} \
#    -l cluster=${CLUSTER} \
#    -p 8080:8080 \
#    -p 8443:8443 \
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
