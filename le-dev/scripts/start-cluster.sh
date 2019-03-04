#!/bin/bash

# Test for required env variables
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"
printf "%s\n" "${SPARK_HOME:?You must set SPARK_HOME}"
printf "%s\n" "${LIVY_HOME:?You must set LIVY_HOME}"

rm -f /tmp/*.out
rm -Rf $HADOOP_HOME/logs/*.log*
rm -Rf $HADOOP_HOME/logs/*.out*
rm -Rf $HADOOP_HOME/logs/userlogs/*

export YARN_RESOURCEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5001,server=y,suspend=n"
export YARN_NODEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5002,server=y,suspend=n"

if [[ -f "/opt/java/default" ]]; then
    export JAVA_HOME="/opt/java/default"
fi

${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode
${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode
${HADOOP_HOME}/sbin/yarn-daemon.sh start resourcemanager
${HADOOP_HOME}/sbin/yarn-daemon.sh start nodemanager
${HADOOP_HOME}/sbin/yarn-daemon.sh start timelineserver
${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh start historyserver

${SPARK_HOME}/sbin/start-history-server.sh
if [[ -n "${J8_HOME}" ]]; then
    JAVA_HOME=${J8_HOME} ${LIVY_HOME}/bin/livy-server start
else
    ${LIVY_HOME}/bin/livy-server start
fi
