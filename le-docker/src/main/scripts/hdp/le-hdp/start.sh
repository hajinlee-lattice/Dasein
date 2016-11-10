#!/usr/bin/env bash

service ssh restart

echo "${NAMENODE}" > $HADOOP_CONF_DIR/slaves

su yarn -c "$HADOOP_HOME/bin/hdfs namenode -format"
su yarn -c "$HADOOP_HOME/sbin/start-dfs.sh"
su yarn -c "$HADOOP_HOME/sbin/start-yarn.sh"
su yarn -c "$HADOOP_HOME/sbin/yarn-daemon.sh start timelineserver"
su yarn -c "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver"

tail -f $HADOOP_HOME/logs/*