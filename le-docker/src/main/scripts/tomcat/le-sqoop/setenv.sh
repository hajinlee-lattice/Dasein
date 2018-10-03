#!/usr/bin/env bash

echo "JAVA_HOME=${JAVA_HOME}"
echo "CATALINA_HOME=${CATALINA_HOME}"

export LE_PROPDIR="/etc/ledp"

export JAVA_OPTS="-Duser.timezone=US/Eastern -Dfile.encoding=UTF8"

if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi
export CATALINA_CLASSPATH="${CLASSPATH}:${HADOOP_CONF_DIR}:${HADOOP_COMMON_DIR}:${JAVA_HOME}/lib/tools.jar"

echo ${JAVA_OPTS}
echo ${CATALINA_CLASSPATH}

export HADOOP_USER_NAME=hdfs
