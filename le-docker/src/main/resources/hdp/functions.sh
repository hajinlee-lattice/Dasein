#!/usr/bin/env bash

source ../functions.sh

function teardown_hdp() {
    CLUSTER=$1
    CLUSTER="${CLUSTER:=hdp}"

    teardown_simple_service "hdp" ${CLUSTER}
    sudo sed -i "s/${CLUSTER}-hdp//g" /etc/hosts
}