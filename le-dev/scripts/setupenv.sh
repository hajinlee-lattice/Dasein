#!/bin/bash

printf "%s\n" "${HADOOP_CONF_DIR:?You must set HADOOP_CONF_DIR}"
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${LE_STACK:?You must set LE_STACK to a unique value among developers}"
printf "%s\n" "${HADOOP_NAMENODE_DATA_DIR:?You must set HADOOP_NAMENODE_DATA_DIR}"
printf "%s\n" "${HADOOP_DATANODE_DATA_DIR:?You must set HADOOP_DATANODE_DATA_DIR}"
printf "%s\n" "${ANACONDA_HOME:?You must set ANACONDA_HOME}"
printf "%s\n" "${AWS_KEY:?You must set AWS_KEY}"
printf "%s\n" "${AWS_SECRET:?You must set AWS_SECRET}"

BOOTSTRAP_MODE=$1

bash $WSHOME/le-dev/scripts/setupenv_anaconda.sh $BOOTSTRAP_MODE && \
bash $WSHOME/le-dev/scripts/setupenv_aws.sh && \
bash $WSHOME/le-dev/scripts/setupenv_git.sh && \
bash $WSHOME/le-dev/scripts/setupenv_tomcat.sh $BOOTSTRAP_MODE && \
bash $WSHOME/le-dev/scripts/setupenv_hdp.sh $BOOTSTRAP_MODE &&
bash $WSHOME/le-dev/scripts/setupenv_spark.sh $BOOTSTRAP_MODE && \
bash $WSHOME/le-dev/scripts/setupenv_docker.sh $BOOTSTRAP_MODE || true

pip install -r $WSHOME/le-dev/scripts/requirements.txt || true
echo "REVIEWBOARD_URL='http://bodcdevvrvw65.lattice.local/rb'" > ~/.reviewboardrc
