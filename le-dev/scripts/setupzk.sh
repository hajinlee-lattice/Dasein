#!/usr/bin/env bash

echo "Setting up Zookeeper"

if [ -d "${ANACONDA_HOME}/envs/p2" ]; then
    source "${ANACONDA_HOME}/bin/activate" p2
else
    source "${ANACONDA_HOME}/bin/activate" lattice
fi

if [ -z `which zookeeper_import` ]; then
    echo "Have you installed zc.zk python package? You can use 'pip install zc.zk' to install it."
    exit 1
fi

PYTHON=${PYTHON:=python}
ZK_HOST="localhost:2181"
LOCAL_TEST_POD="/Pods/Default/Contracts"
$PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print '${LOCAL_TEST_POD} exists' if zk.exists('${LOCAL_TEST_POD}') else zk.create_recursive('${LOCAL_TEST_POD}', '', zc.zk.OPEN_ACL_UNSAFE)"

function bootstrap_tenant() {
	TENANT=$1
    LOCAL_TEST_ROOT="/Pods/Default/Contracts/${TENANT}"
    $PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print zk.delete_recursive('${LOCAL_TEST_ROOT}') if zk.exists('${LOCAL_TEST_ROOT}') else '${LOCAL_TEST_ROOT} is empty';"
    zookeeper_import ${ZK_HOST} $WSHOME/le-dev/testartifacts/zookeeper/${TENANT}.txt ${LOCAL_TEST_POD}
    $PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}'); print zk.export_tree('${LOCAL_TEST_ROOT}')"
}

bootstrap_tenant LocalTest
bootstrap_tenant LocalTest2

DATA_FABRIC_POD="/Pods/FabricConnectors"
$PYTHON -c "import zc.zk; zk = zc.zk.ZooKeeper('${ZK_HOST}');  print '${DATA_FABRIC_POD} exists' if zk.exists('${DATA_FABRIC_POD}') else zk.create_recursive('${DATA_FABRIC_POD}', '', zc.zk.OPEN_ACL_UNSAFE)"

source "${ANACONDA_HOME}/bin/deactivate"
