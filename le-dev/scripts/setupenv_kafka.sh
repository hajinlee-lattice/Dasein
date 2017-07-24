#!/usr/bin/env bash

printf "%s\n" "${WSHOME:?Must set CONFLUENT_HOME to your root dir for confluent platform}"

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping kafka via confluent platform ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    CP_MAJOR=3.2
    CP_VERSION=3.2.2
    CP_SCALA_VERSION=2.11
    CP_TGZ_URL="http://packages.confluent.io/archive/${CP_MAJOR}/confluent-oss-${CP_VERSION}-${CP_SCALA_VERSION}.tar.gz"

    sudo rm -rf ${CONFLUENT_HOME}
    sudo mkdir -p ${CONFLUENT_HOME} || true
    sudo chown -R ${USER} ${CONFLUENT_HOME} || true

    if [ ! -f "${ARTIFACT_DIR}/confluent-oss-${CP_VERSION}-${CP_SCALA_VERSION}.tar.gz" ]; then
        wget "${CP_TGZ_URL}" -O "${ARTIFACT_DIR}/confluent-oss-${CP_VERSION}-${CP_SCALA_VERSION}.tar.gz"
    fi
    rm -rf ${ARTIFACT_DIR}/confluent-${CP_VERSION} || true
    tar xzf ${ARTIFACT_DIR}/confluent-oss-${CP_VERSION}-${CP_SCALA_VERSION}.tar.gz -C ${ARTIFACT_DIR}
    cp -rf ${ARTIFACT_DIR}/confluent-${CP_VERSION}/* ${CONFLUENT_HOME}
fi

cp ${WSHOME}/le-dev/kafka/server.properties ${CONFLUENT_HOME}/etc/kafka/server.properties
cp ${WSHOME}/le-dev/kafka/schema-registry.properties ${CONFLUENT_HOME}/etc/schema-registry/schema-registry.properties
cp ${WSHOME}/le-dev/kafka/connect-avro-standalone.properties ${CONFLUENT_HOME}/etc/schema-registry/connect-avro-standalone.properties

sudo mkdir -p /var/log/kafka || true
sudo chown ${USER} /var/log/kafka
chmod +w /var/log/kafka

mkdir -p ${CONFLUENT_HOME}/etc/datafabric