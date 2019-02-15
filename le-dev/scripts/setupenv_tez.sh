#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    TEZ_VERSION=0.9.1

    if [[ ! -f "$ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz" ]]; then
        wget --trust-server-names "https://s3.amazonaws.com/latticeengines-dev/artifacts/tez/${TEZ_VERSION}/tez-${TEZ_VERSION}.tar.gz" \
            -O $ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz
    fi
    hdfs dfsadmin -safemode leave
    hdfs dfs -mkdir -p /apps/tez || true
    hdfs dfs -put -f $ARTIFACT_DIR/tez-${TEZ_VERSION}.tar.gz /apps/tez/tez-${TEZ_VERSION}.tar.gz
fi

cp $WSHOME/le-dev/hadoop/tez-site.xml $HADOOP_CONF_DIR
cp $WSHOME/le-dev/hadoop/log4j2-tez.xml $HADOOP_CONF_DIR
