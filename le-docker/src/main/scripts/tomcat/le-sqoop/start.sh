#!/usr/bin/env bash
mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat ${CATALINA_HOME}

ulimit -n 4096

export JAVA_OPTS="-Dlog4j.configurationFile=${CATALINA_HOME}/conf/log4j2.xml"

${CATALINA_HOME}/bin/catalina.sh run


