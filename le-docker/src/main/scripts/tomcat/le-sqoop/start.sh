#!/usr/bin/env bash
echo "JAVA_HOME=${JAVA_HOME}"
echo "CATALINA_HOME=${CATALINA_HOME}"

export LE_PROPDIR="/etc/ledp"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

export JAVA_OPTS="-Duser.timezone=US/Eastern -Dfile.encoding=UTF8"

if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi
export CATALINA_CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR

echo ${JAVA_OPTS}

mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat ${CATALINA_HOME}

ulimit -n 10240

${CATALINA_HOME}/bin/catalina.sh run


