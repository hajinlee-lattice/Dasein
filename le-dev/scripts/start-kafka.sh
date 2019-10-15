#!/usr/bin/env bash

# Test for required env variables
if [ -z "${CONFLUENT_HOME}" ]; then
    echo "Must set CONFLUENT_HOME to your root dir for confluent platform"
    exit -1
fi

rm -rf ${CONFLUENT_HOME}/logs/*

DEV_KAFKA_DIR=${WSHOME}/le-dev/kafka

echo "Starting kafka broker ..."
${CONFLUENT_HOME}/bin/kafka-server-start -daemon ${DEV_KAFKA_DIR}/server.properties

echo "Sleep 3 sec, and check tail of broker log ${CONFLUENT_HOME}/logs/kafkaServer.out ..."
sleep 3
tail -n 50 ${CONFLUENT_HOME}/logs/kafkaServer.out

echo "Starting schema registry ..."
${CONFLUENT_HOME}/bin/schema-registry-start ${DEV_KAFKA_DIR}/schema-registry.properties > ${CONFLUENT_HOME}/logs/schemaRegistry.out &

echo "Sleep 3 sec, and check tail of schema registry log ${CONFLUENT_HOME}/logs/schemaRegistry.ou ..."
sleep 3
tail -n 50 ${CONFLUENT_HOME}/logs/schemaRegistry.out

echo "If you see errors in above log tails, please consider restart the servers."
