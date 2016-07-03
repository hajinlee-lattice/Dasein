#!/usr/bin/env bash

source ../functions.sh

function teardown_kafka() {

    CLUSTER=$1
    CLUSTER="${CLUSTER:=kafka}"

    echo "tearing down kafka cluster named ${CLUSTER} ..."

    deregister_by_label "kafka.group" ${CLUSTER} "kafka-broker"
    deregister_by_label "kafka.group" ${CLUSTER} "schema-registry"
    deregister_by_label "kafka.group" ${CLUSTER} "kafka-connect"

    CLUSTER=${CLUSTER} \
    BK_PORT=9092 \
    SR_PORT=9022 \
    KC_PORT=9023 \
    NETWORK="lenet" \
    GATEWAY="172.17.0.1" \
    docker-compose -p ${CLUSTER} down --remove-orphans
    rm_by_label "kafka.group" ${CLUSTER}
}