#!/usr/bin/env bash

export CATALINA_HOME=/usr/local/tomcat
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

echo LE_ENVIRONMENT=${LE_ENVIRONMENT}
export LE_PROPDIR=/etc/ledp/${LE_ENVIRONMENT}

export JAVA_OPTS="-Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1099"
export JAVA_OPTS="${JAVAOPTS} ${CATALINA_OPTS}"
export CATALINA_CLASSPATH=$CLASSPATH:$TEZ_CONF_DIR:$HADOOP_HOME/etc/hadoop:$JAVA_HOME/lib/tools.jar:$HADOOP_COMMON_JAR

${CATALINA_HOME}/bin/catalina.sh run