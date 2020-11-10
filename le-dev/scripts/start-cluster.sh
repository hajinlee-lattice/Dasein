#!/bin/bash

# Test for required env variables
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"
printf "%s\n" "${SPARK_HOME:?You must set SPARK_HOME}"
printf "%s\n" "${LIVY_HOME:?You must set LIVY_HOME}"

rm -f /tmp/*.out
rm -Rf "${HADOOP_HOME}"/logs/*.log*
rm -Rf "${HADOOP_HOME}"/logs/*.out*
rm -Rf "${HADOOP_HOME}"/logs/userlogs/*

export YARN_RESOURCEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5001,server=y,suspend=n"
export YARN_NODEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5002,server=y,suspend=n"

if [[ -f "/opt/java/default" ]]; then
    export JAVA_HOME="/opt/java/default"
fi

HACK_LINE="127.0.0.1 ${HOSTNAME}"
if [[ -n $(ps aux | grep "java -jar remoting.jar" | grep -v grep) ]]; then
  echo "This is a jenkins slave."
else
  echo "sudo required to fix /etc/hosts during cluster startup"
  echo ${HACK_LINE} | sudo tee -a /etc/hosts
fi

"${HADOOP_HOME}/sbin/hadoop-daemon.sh" start namenode
"${HADOOP_HOME}/sbin/hadoop-daemon.sh" start datanode
"${HADOOP_HOME}/sbin/yarn-daemon.sh" start resourcemanager
"${HADOOP_HOME}/sbin/yarn-daemon.sh" start nodemanager
"${HADOOP_HOME}/sbin/yarn-daemon.sh" start timelineserver
"${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh" start historyserver

echo "Removing /etc/hosts hack"
UNAME=`uname`
if [[ "$UNAME" == 'Darwin' ]]; then
    sudo sed -i '' "/^${HACK_LINE}$/d" /etc/hosts
else
    sudo sed -i "/^${HACK_LINE}$/d" /etc/hosts
fi

"${SPARK_HOME}"/sbin/start-history-server.sh

if [[ -n "${J8_HOME}" ]]; then
    JAVA_HOME="${J8_HOME}" "${LIVY_HOME}/bin/livy-server" start
else
    "${LIVY_HOME}/bin/livy-server" start
fi

if [[ -n "${HIVE_HOME}" ]]; then
  ps -ef | grep HiveMetaStore > hive_process
  pid=$(cat hive_process | grep org.apache.hadoop.hive.metastore.HiveMetaStore | cut -d ' ' -f 2)
  if [[ -n ${pid} ]]; then
    echo "Found hive metastore pid ${pid}, going to kill it"
    kill -9 ${pid}
  fi
  pushd ${HIVE_HOME}
  nohup bin/hiveserver2 --service metastore > log/metastore.log &
  popd

  if [[ -n "${PRESTO_HOME}" ]]; then
    pushd ${PRESTO_HOME}
    bin/launcher start
    popd
  fi

fi
