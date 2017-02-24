#!/bin/bash

# Test for required env variables
printf "%s\n" "${HADOOP_HOME:?You must set HADOOP_HOME}"

if [ "${LE_USING_DOCKER}" != "true" ]; then
    printf "%s\n" "${ZOOKEEPER_HOME:?You must set ZOOKEEPER_HOME}"
    printf "%s\n" "${DYNAMO_HOME:?You must set DYNAMO_HOME}"
fi

rm -f /tmp/*.out
rm -Rf $HADOOP_HOME/logs/*.log*
rm -Rf $HADOOP_HOME/logs/*.out*
rm -Rf $HADOOP_HOME/logs/userlogs/*

export YARN_RESOURCEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5001,server=y,suspend=n"
export YARN_NODEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5002,server=y,suspend=n"

$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start timelineserver
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

OLD_CATALINA_HOME=$CATALINA_HOME
CATALINA_HOME=$HADOOP_HOME/share/hadoop/kms/tomcat
$HADOOP_HOME/sbin/kms.sh start
CATALINA_HOME=$OLD_CATALINA_HOME

if [ "${LE_USING_DOCKER}" = "true" ]; then
    echo "You are in Docker environment, please use dk-start to start docker compose pod"
else
    ZOO_LOG_DIR=$ZOOKEEPER_HOME/logs $ZOOKEEPER_HOME/bin/zkServer.sh start
    echo "Starting Dynamo"
    nohup java -Djava.library.path=${DYNAMO_HOME}/DynamoDBLocal_lib -jar ${DYNAMO_HOME}/DynamoDBLocal.jar -dbPath ${DYNAMO_HOME} -sharedDb > ${DYNAMO_HOME}/dynamo.log 2>&1 &
fi

