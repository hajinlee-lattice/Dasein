#!/usr/bin/env bash

CLUSTER=$1

CLUSTER="${CLUSTER:=tomcat}"

source ../functions.sh
teardown_simple_service "matchapi" $CLUSTER
teardown_simple_service "oauth2" $CLUSTER
teardown_simple_service "playmaker" $CLUSTER
teardown_simple_service "scoringapi" $CLUSTER
teardown_simple_service "admin" $CLUSTER
teardown_simple_service "pls" $CLUSTER