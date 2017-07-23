#!/usr/bin/env bash

if [ -z "${LE_ENVIRONMENT}" ]; then
    export LE_ENVIRONMENT=dev
fi

if [ -z "${LE_CLIENT_ADDRESS}" ]; then
    export LE_CLIENT_ADDRESS=localhost
fi

export LE_PROPDIR=$WSHOME/le-config/conf/env/${LE_ENVIRONMENT}
echo "Using LE_PROPDIR=${LE_PROPDIR}"
echo "Using LE_CLIENT_ADDRESS=${LE_CLIENT_ADDRESS}"

export JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=n"
export JAVA_OPTS="${JAVA_OPTS} -Dsqoop.throwOnError=true -Xmx4g -XX:ReservedCodeCacheSize=512m"
export JAVA_OPTS="${JAVA_OPTS} -Djavax.net.ssl.trustStore=${WSHOME}/le-security/certificates/ledp_keystore.jks"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1098"
export CATALINA_CLASSPATH=$CLASSPATH:$TEZ_CONF_DIR:$HADOOP_HOME/etc/hadoop:$JAVA_HOME/lib/tools.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.1.2.4.3.0-227.jar

if [ $# -eq 0 ]; then
    echo "Starting tomcat normally..."
    $CATALINA_HOME/bin/catalina.sh run
elif [ $1 == "daemon" ]; then
    echo "Starting tomcat as daemon..."
    $CATALINA_HOME/bin/startup.sh
fi


