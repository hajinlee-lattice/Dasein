#!/usr/bin/env bash

if [ -z "$(which zookeeper_import)" ]; then
    echo "Have you installed zc.zk python package? You can use 'pip install zc.zk' to install it."
    exit 1
fi

PYTHON=${PYTHON:=python}
ZK_HOST="localhost:2181"
LOCAL_TEST_ROOT="/Pods/Default/Contracts/LocalTest"
LOCAL_TEST_POD="/Pods/Default/Contracts"

$PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print zk.delete_recursive('${LOCAL_TEST_ROOT}') if zk.exists('${LOCAL_TEST_ROOT}') else '${LOCAL_TEST_ROOT} is empty'; 
print 'LOCAL_TEST_POD exists' if zk.exists('${LOCAL_TEST_POD}') else zk.create_recursive('${LOCAL_TEST_POD}', '', zc.zk.OPEN_ACL_UNSAFE)"

zookeeper_import ${ZK_HOST} $WSHOME/le-dev/testartifacts/zookeeper/LocalTest.txt ${LOCAL_TEST_POD}

$PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print zk.export_tree('${LOCAL_TEST_ROOT}')"