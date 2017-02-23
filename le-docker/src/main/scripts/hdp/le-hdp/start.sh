#!/usr/bin/env bash

service ssh restart

rm -f /tmp/*.out
rm -Rf $HADOOP_HOME/logs/*.log*
rm -Rf $HADOOP_HOME/logs/*.out*
rm -Rf $HADOOP_HOME/logs/userlogs/*

# export YARN_RESOURCEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5001,server=y,suspend=n"
# export YARN_NODEMANAGER_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5002,server=y,suspend=n"

su yarn -c "$HADOOP_HOME/bin/hdfs namenode -format"
su yarn -c "$HADOOP_HOME/sbin/start-dfs.sh"
su yarn -c "$HADOOP_HOME/sbin/start-yarn.sh"
su yarn -c "$HADOOP_HOME/sbin/yarn-daemon.sh start timelineserver"
su yarn -c "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver"
su yarn -c "$HADOOP_HOME/sbin/kms.sh start"

sleep 10

tail -f $HADOOP_HOME/logs/*