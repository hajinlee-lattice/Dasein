#!/bin/bash

# Test for required env variables
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"
#printf "%s\n" "${SPARK_HOME:?You must set SPARK_HOME}"
#printf "%s\n" "${LIVY_HOME:?You must set LIVY_HOME}"

${HADOOP_HOME}/sbin/hadoop-daemon.sh stop namenode
${HADOOP_HOME}/sbin/hadoop-daemon.sh stop datanode
${HADOOP_HOME}/sbin/yarn-daemon.sh stop resourcemanager
${HADOOP_HOME}/sbin/yarn-daemon.sh stop nodemanager
${HADOOP_HOME}/sbin/yarn-daemon.sh stop timelineserver
${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh stop historyserver
${HADOOP_HOME}/sbin/kms.sh stop


if [ "${SPARK_HOME}" != "" ]; then
    ${SPARK_HOME}/sbin/stop-history-server.sh
fi
if [ "${LIVY_HOME}" != "" ]; then
    ${LIVY_HOME}/bin/livy-server stop
fi
