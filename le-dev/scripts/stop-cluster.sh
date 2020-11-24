#!/bin/bash

# Test for required env variables
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"
printf "%s\n" "${SPARK_HOME:?You must set SPARK_HOME}"
printf "%s\n" "${LIVY_HOME:?You must set LIVY_HOME}"

"${HADOOP_HOME}/sbin/hadoop-daemon.sh" stop namenode
"${HADOOP_HOME}/sbin/hadoop-daemon.sh" stop datanode
"${HADOOP_HOME}/sbin/yarn-daemon.sh" stop resourcemanager
"${HADOOP_HOME}/sbin/yarn-daemon.sh" stop nodemanager
"${HADOOP_HOME}/sbin/yarn-daemon.sh" stop timelineserver
"${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" stop historyserver

"${SPARK_HOME}/sbin/stop-history-server.sh"
"${LIVY_HOME}/bin/livy-server" stop

if [[ -n "${HIVE_HOME}" ]]; then
  ps -ef | grep HiveMetaStore > hive_process
  pid=$(cat hive_process | grep org.apache.hadoop.hive.metastore.HiveMetaStore | cut -d ' ' -f 2)
  if [[ -n ${pid} ]]; then
    echo "Found hive metastore pid ${pid}, going to kill it"
    kill -9 ${pid}
  fi
fi

if [[ -n "${PRESTO_HOME}" ]]; then
  ${PRESTO_HOME}/bin/launcher stop
fi
