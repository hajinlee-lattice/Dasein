#!/usr/bin/env bash

export CATALINA_HOME=/opt/apache-tomcat-8.5.8
export JAVA_HOME=/usr/java/default
export LE_PROPDIR="/etc/ledp"
export QUARTZ_EXECUTION_HOST="localhost"

echo "QUARTZ_EXECUTION_HOST=${QUARTZ_EXECUTION_HOST}"

export JAVA_OPTS="-Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1099"
if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi

echo ${JAVA_OPTS}

for d in 'ms' 'pls' 'admin' 'playmaker' 'oauth' 'scoringapi' 'mathcapi' 'ulysses' 'api'
do
    echo "checking webapps ${d} ..."
    if [ ! -d "${CATALINA_HOME}/webapps/${d}" ]; then
        echo "webapps/${d} not exists"
        mkdir -p ${CATALINA_HOME}/webapps/${d}
    fi
    if [ ! -d "${CATALINA_HOME}/webapps/${d}/manager" ]; then
        echo "webapps/${d}/manager not exists"
        cp -r /manager ${CATALINA_HOME}/webapps/${d}/manager
    fi
done

bash /start-proxy.sh
${CATALINA_HOME}/bin/catalina.sh run