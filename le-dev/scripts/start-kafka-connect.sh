#!/usr/bin/env bash

DEV_KAFKA_DIR=${WSHOME}/le-dev/kafka

echo "Starting standalone connect worker ..."
WORKER_PROPS=${DEV_KAFKA_DIR}/connect-avro-standalone.properties

CONNECT_GENERIC_PROPS=${CONFLUENT_HOME}/etc/datafabric/datafabric-connect-generic.properties

rm ${CONNECT_GENERIC_PROPS} || true

sed "s|{{HADOOP_CONF_DIR}}|${HADOOP_CONF_DIR}|g" ${DEV_KAFKA_DIR}/datafabric-connect-generic.properties > ${CONNECT_GENERIC_PROPS}

CLASSPATH=${CONFLUENT_HOME}/connectors/* \
    ${CONFLUENT_HOME}/bin/connect-standalone \
    ${WORKER_PROPS} \
    ${CONNECT_GENERIC_PROPS}