#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="saml"

source ../functions.sh

#docker run -d --net host \
#    --name ${CLUSTER}_${SERVICE} \
#    -h ${HOSTNAME} \
#    -e LE_ENVIRONMENT=dev \
#    -e LE_STACK=${LE_STACK} \
#    -e ENABLE_JACOCO=false \
#    -e DISABLE_JMXTRANS=true \
#    -e CATALINA_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m" \
#    -v ${WSHOME}/le-config/conf/env/dev/latticeengines.properties:/etc/ledp/latticeengines.properties \
#    -v ${WSHOME}/le-docker/src/main/scripts/tomcat/jacoco:/mnt/efs/jacoco:rw \
#    -l ${SERVICE}.group=${CLUSTER} \
#    -l cluster=${CLUSTER} \
#    latticeengines/${SERVICE}

docker run -d \
    --name ${CLUSTER} \
    -h ${CLUSTER}-tomcat \
    -e LE_ENVIRONMENT=dev \
    -e LE_STACK=${LE_STACK} \
    -e ENABLE_JACOCO=false \
    -e DISABLE_JMXTRANS=true \
    -l ${SERVICE}.group=${CLUSTER} \
    -l cluster=${CLUSTER} \
    -p 80:8080 \
    -p 443:8443 \
    -p 1098:1098 \
    -p 1099:1099 \
    latticeengines/tomcatbase-j8

docker_ps
