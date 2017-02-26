#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    if [ ! -f "$ARTIFACT_DIR/tez-0.8.2.tar.gz" ]; then
        wget https://s3.amazonaws.com/latticeengines-dev/tez-0.8.2.tar.gz -O $ARTIFACT_DIR/tez-0.8.2.tar.gz
    fi
    hdfs dfsadmin -safemode leave
    hdfs dfs -mkdir -p /apps/tez || true
    hdfs dfs -put -f $ARTIFACT_DIR/tez-0.8.2.tar.gz /apps/tez/tez-0.8.2.tar.gz
fi