#!/usr/bin/env bash

if [[ -z "${LE_ENVIRONMENT}" ]]; then
    export LE_ENVIRONMENT=dev
fi

if [[ -z "${LE_CLIENT_ADDRESS}" ]]; then
    export LE_CLIENT_ADDRESS=localhost
fi

export LE_PROPDIR=$WSHOME/le-config/conf/env/${LE_ENVIRONMENT}
echo "Using LE_PROPDIR=${LE_PROPDIR}"
echo "Using LE_CLIENT_ADDRESS=${LE_CLIENT_ADDRESS}"

if [[ ! -z "${J11_HOME}" ]]; then
    export JAVA_HOME=${J11_HOME}
fi

if [[ ! -z $(java -version 2>&1 |  grep "11.0") ]]; then
    echo "Java version: $(java -version 2>&1)"
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UnlockExperimentalVMOptions"
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+EnableJVMCI -XX:+UseJVMCICompiler"
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseZGC"
else
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseG1GC"
fi

export JAVA_OPTS="-server -Xmx4g -XX:ReservedCodeCacheSize=512m"
export JAVA_OPTS="${JAVA_OPTS} -Djava.net.preferIPv4Stack=true"
export JAVA_OPTS="${JAVA_OPTS} -Dsqoop.throwOnError=true"
export JAVA_OPTS="${JAVA_OPTS} -Djava.library.path=${CATALINA_HOME}/lib"
export JAVA_OPTS="${JAVA_OPTS} -Djavax.net.ssl.trustStore=/etc/ledp/tls/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1098"
export JAVA_OPTS="${JAVA_OPTS} -Dio.lettuce.core.topology.sort=RANDOMIZE"

# uncomment this line to enable remote debugger
# export JAVA_OPTS="${JAVA_OPTS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4001"

if [[ "${ENABLE_JACOCO}" == "true" ]]; then
    JACOCO_DEST_FILE="${WSHOME}/jacoco/tomcat.exec"
    JACOCO_AGENT_FILE="${WSHOME}/le-dev/jacocoagent.jar"
    export JAVA_OPTS="${JAVA_OPTS} -javaagent:${JACOCO_AGENT_FILE}=includes=com.latticeengines.*,destfile=${JACOCO_DEST_FILE},append=true,jmx=true"
fi

export CATALINA_CLASSPATH=${CLASSPATH}:${TEZ_CONF_DIR}:${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common

if [[ $# -eq 0 ]]; then
    echo "Starting tomcat normally..."
    ${CATALINA_HOME}/bin/catalina.sh run
elif [[ $1 == "daemon" ]]; then
    echo "Starting tomcat as daemon..."
    ${CATALINA_HOME}/bin/startup.sh
fi


