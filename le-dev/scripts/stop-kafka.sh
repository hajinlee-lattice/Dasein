#!/usr/bin/env bash

# Test for required env variables
if [ -z "${CONFLUENT_HOME}" ]; then
    echo "Must set CONFLUENT_HOME to your root dir for confluent platform"
    exit -1
fi

echo "Stopping schema registry ..."
${CONFLUENT_HOME}/bin/schema-registry-stop

echo "Stopping kafka broker ..."
${CONFLUENT_HOME}/bin/kafka-server-stop
