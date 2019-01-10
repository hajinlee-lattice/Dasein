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

if [[ -n "${J11_HOME}" ]]; then
    export JAVA_HOME=${J11_HOME}
fi

export CATALINA_OPTS="-server -Xmx4g -XX:ReservedCodeCacheSize=512m"
export CATALINA_OPTS="${CATALINA_OPTS} -Djava.net.preferIPv4Stack=true"
export CATALINA_OPTS="${CATALINA_OPTS} -Dsqoop.throwOnError=true"
export CATALINA_OPTS="${CATALINA_OPTS} -Djava.library.path=${CATALINA_HOME}/lib"
export CATALINA_OPTS="${CATALINA_OPTS} -Djavax.net.ssl.trustStore=/etc/ledp/tls/cacerts"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1098"
export CATALINA_OPTS="${CATALINA_OPTS} -Dio.lettuce.core.topology.sort=RANDOMIZE"

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseNUMA"
if [[ -n $(java -version 2>&1 |  grep "11.0") ]]; then
    echo "Java version: $(java -version 2>&1)"
    if [[ -n "${JPDA_PORT}" ]]; then
        export CATALINA_OPTS="${CATALINA_OPTS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:4001"
        export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseParallelGC"
    else
        export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UnlockExperimentalVMOptions"
        export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+EnableJVMCI -XX:+UseJVMCICompiler"
        export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseZGC"
    fi
else
    if [[ -n "${JPDA_PORT}" ]]; then
        export CATALINA_OPTS="${CATALINA_OPTS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4001"
    else
        export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseG1GC"
    fi
fi

if [[ "${ENABLE_JACOCO}" == "true" ]]; then
    JACOCO_DEST_FILE="${WSHOME}/jacoco/tomcat.exec"
    JACOCO_AGENT_FILE="${WSHOME}/le-dev/jacocoagent-0.8.2.jar"
    export CATALINA_OPTS="${CATALINA_OPTS} -javaagent:${JACOCO_AGENT_FILE}=includes=com.latticeengines.*,destfile=${JACOCO_DEST_FILE},append=true,jmx=true"
fi

export CATALINA_CLASSPATH=${CLASSPATH}:${TEZ_CONF_DIR}:${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common

if [[ $# -eq 0 ]]; then
    echo "Starting tomcat normally..."
    ${CATALINA_HOME}/bin/catalina.sh run
elif [[ $1 == "daemon" ]]; then
    echo "Starting tomcat as daemon..."
    ${CATALINA_HOME}/bin/startup.sh
fi


