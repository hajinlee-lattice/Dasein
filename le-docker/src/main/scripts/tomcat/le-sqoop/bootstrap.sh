#!/usr/bin/env bash

CLUSTER=$1
NETWORK=$3

CLUSTER="${CLUSTER:=tomcat}"
NETWORK="${NETWORK:=lenet}"

SERVICE="sqoop"

source ../../functions.sh

#SQOOP_CLASSPATH="${CLASSPATH}:${TEZ_CONF_DIR}:${HADOOP_HOME}/etc/hadoop:${JAVA_HOME}/lib/tools.jar:${HADOOP_HOME}/share/hadoop/common"
#SQOOP_CLASSPATH="${SQOOP_CLASSPATH}:/etc/hadoop/conf:${HADOOP_CLASSPATH}"

docker run -d --rm \
    --name sqoop \
    -p 80:8080 \
    -p 443:8443 \
    latticeengines/sqoop

docker_ps
