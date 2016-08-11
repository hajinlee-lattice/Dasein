#!/usr/bin/env bash

CLUSTER=$1

CLUSTER="${CLUSTER:=tomcat}"

source ../functions.sh
teardown_simple_service "matchapi" $CLUSTER