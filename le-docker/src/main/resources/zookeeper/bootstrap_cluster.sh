#!/usr/bin/env bash

source functions.sh


CLUSTER="zookeeper"

teardown_zookeeper ${CLUSTER}

CLUSTER=${CLUSTER} \
docker-compose -p ${CLUSTER} up -d --remove-orphans

sleep 2
docker_ps