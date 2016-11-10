#!/usr/bin/env bash

su yarn -c "$HADOOP_HOME/bin/hdfs dfs -rm -r -f /apps/tez"
su yarn -c "$HADOOP_HOME/bin/hdfs dfs -mkdir -p /apps/tez"
su yarn -c "$HADOOP_HOME/bin/hdfs dfs -put /tez-0.8.2.tar.gz /apps/tez"