#!/usr/bin/env bash

if [ -z "$(which zookeeper_import)" ]; then
    echo "Have you installed zc.zk python package? You can use 'pip install zc.zk' to install it."
    exit 1
fi

PYTHON=${PYTHON:=python}
ZK_HOST="localhost:2181"
LOCAL_TEST_ROOT="/Pods/Default/Contracts/LocalTest"

$PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print zk.delete_recursive('${LOCAL_TEST_ROOT}') if zk.exists('${LOCAL_TEST_ROOT}') else '${LOCAL_TEST_ROOT} is empty'"

zookeeper_import ${ZK_HOST} $WSHOME/le-dev/testartifacts/zookeeper/LocalTest.txt /Pods/Default/Contracts

$PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print zk.export_tree('${LOCAL_TEST_ROOT}')"