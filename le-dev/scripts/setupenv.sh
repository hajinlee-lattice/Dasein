#!/bin/bash

printf "%s\n" "${HADOOP_CONF_DIR:?You must set HADOOP_CONF_DIR}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${LE_STACK:?You must set LE_STACK to a unique value among developers}"
printf "%s\n" "${HADOOP_NAMENODE_DATA_DIR:?You must set HADOOP_NAMENODE_DATA_DIR}"
printf "%s\n" "${HADOOP_DATANODE_DATA_DIR:?You must set HADOOP_DATANODE_DATA_DIR}"
printf "%s\n" "${ANACONDA_HOME:?You must set ANACONDA_HOME}"

if [ "${LE_USING_DOCKER}" != "true" ]; then
    printf "%s\n" "${DYNAMO_HOME:?You must set DYNAMO_HOME}"
fi

BOOTSTRAP_MODE=$1

pip install -r $WSHOME/le-dev/scripts/requirements.txt || true
echo "REVIEWBOARD_URL='http://bodcdevvrvw65.lattice.local/rb'" > ~/.reviewboardrc

bash $WSHOME/le-dev/scripts/setupenv_aws.sh || true
bash $WSHOME/le-dev/scripts/setupenv_hdp.sh $BOOTSTRAP_MODE || true
bash $WSHOME/le-dev/scripts/setupenv_tomcat.sh $BOOTSTRAP_MODE || true
bash $WSHOME/le-dev/scripts/setupenv_dynamo.sh $BOOTSTRAP_MODE || true
bash $WSHOME/le-dev/scripts/setupenv_anaconda.sh $BOOTSTRAP_MODE || true

bash $WSHOME/le-dev/scripts/start-cluster.sh || true

if [ "${BOOTSTRAP_MODE}" = "true" ]; then
    for app in 'dataplatform' 'sqoop' 'eai' 'dataflow' 'dataflowapi' 'datacloud' 'workflowapi' 'scoring' 'dellebi'
    do
        hdfs dfs -mkdir -p /app/${app} || true &
    done
    wait
fi

existing=$(hadoop key list | grep master)
if [ -z "$existing" ]; then
    hadoop key create master
else
    echo "Master key already installed"
fi
bash $WSHOME/le-dev/scripts/setupenv_tez.sh $BOOTSTRAP_MODE || true
bash $WSHOME/le-dev/scripts/stop-cluster.sh || true

bash $WSHOME/le-dev/scripts/setupenv_docker.sh $BOOTSTRAP_MODE || true

