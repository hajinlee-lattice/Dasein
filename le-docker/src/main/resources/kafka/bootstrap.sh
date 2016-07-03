#!/bin/bash

CLUSTER=$1    # cluster name
BK_PORT=$2    # Broker
SR_PORT=$3    # Schema Registry
KC_PORT=$4    # Kafka Connect
NETWORK=$5    # docker network

CLUSTER="${CLUSTER:=kafka}"
BK_PORT="${BK_PORT:=9092}"
SR_PORT="${SR_PORT:=9022}"
KC_PORT="${KC_PORT:=9024}"
NETWORK="${NETWORK:=lenet}"

source functions.sh

verify_consul
teardown_kafka ${CLUSTER}
create_network ${NETWORK}

GATEWAY=$(docker network inspect ${NETWORK} | grep Gateway | cut -d : -f 2 | cut -d / -f 1 | cut -d \" -f 2)

CLUSTER=${CLUSTER} \
BK_PORT=${BK_PORT} \
SR_PORT=${SR_PORT} \
KC_PORT=${KC_PORT} \
NETWORK=${NETWORK} \
GATEWAY=${GATEWAY} \
docker-compose -p ${CLUSTER} up -d --remove-orphans

register_service "kafka-broker" ${CLUSTER}_bkr_1 9092
register_service "schema-registry" ${CLUSTER}_sr_1 9022
register_service "kafka-connect" ${CLUSTER}_conn_1 9024

sleep 2
docker_ps

show_service "kafka-broker"
show_service "schema-registry"
show_service "kafka-connect"