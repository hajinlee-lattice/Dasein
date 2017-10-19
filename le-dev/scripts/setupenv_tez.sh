#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    TEZ_VERSION=0.9.0

    if [ ! -f "$ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz" ]; then
        wget --trust-server-names "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=tez/${TEZ_VERSION}/apache-tez-${TEZ_VERSION}-bin.tar.gz" -O $ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz
    fi
    hdfs dfsadmin -safemode leave
    hdfs dfs -mkdir -p /apps/tez || true
    hdfs dfs -put -f $ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz /apps/tez/tez-${TEZ_VERSION}.tar.gz
fi