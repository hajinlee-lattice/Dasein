#!/usr/bin/env bash

source ../functions.sh

CLUSTER=$1
CLUSTER="${CLUSTER:=consul}"

echo "tearing down consul cluster named ${CLUSTER} ..."

rm_by_label "consul.group" ${CLUSTER}