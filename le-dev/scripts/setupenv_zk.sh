#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${LE_USING_DOCKER}" != "true" ] && [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    printf "%s\n" "${ZOOKEEPER_HOME:?You must set ZOOKEEPER_HOME}"

    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    sudo rm -rf $ZOOKEEPER_HOME || true
    sudo mkdir -p $ZOOKEEPER_HOME
    sudo chown -R $USER $ZOOKEEPER_HOME

    if [ ! -f "$ARTIFACT_DIR/zookeeper-3.4.9.tar.gz" ]; then
        wget http://apache.claz.org/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz -O $ARTIFACT_DIR/zookeeper-3.4.9.tar.gz
    fi

    tar xzf $ARTIFACT_DIR/zookeeper-3.4.9.tar.gz -C $ARTIFACT_DIR
    cp -r $ARTIFACT_DIR/zookeeper-3.4.9/* $ZOOKEEPER_HOME
    rm -rf $ARTIFACT_DIR/zookeeper-3.4.9

    cp -f $WSHOME/le-dev/zookeeper/zoo.cfg $ZOOKEEPER_HOME/conf
    sed -i".orig" "s|[$][{]ZOOKEEPER_HOME[}]|${ZOOKEEPER_HOME}|" $ZOOKEEPER_HOME/conf/zoo.cfg
fi