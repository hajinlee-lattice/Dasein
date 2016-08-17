#!/usr/bin/env bash

source ../functions.sh

function teardown_zookeeper() {

    CLUSTER=$1
    CLUSTER="${CLUSTER:=zookeeper}"

    echo "tearing down zookeeper cluster named ${CLUSTER} ..."

    CLUSTER=${CLUSTER} \
    docker-compose -p ${CLUSTER} down --remove-orphans
}