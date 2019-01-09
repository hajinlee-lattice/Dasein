#!/usr/bin/env bash

echo "JAVA_HOME=${JAVA_HOME}"
echo "CATALINA_HOME=${CATALINA_HOME}"

export LE_PROPDIR="/etc/ledp"

export CATALINA_OPTS="-Duser.timezone=UTC -Dfile.encoding=UTF8 -Dlog4j.configurationFile=${CATALINA_HOME}/conf/log4j2.xml"
export CLASSPATH="${HADOOP_CONF_DIR}:${HADOOP_COMMON_DIR}:${JAVA_HOME}/lib/tools.jar"

echo ${CATALINA_OPTS}

export HADOOP_USER_NAME=hdfs
