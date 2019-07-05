#!/usr/bin/env bash

echo "JAVA_HOME=${JAVA_HOME}"
echo "CATALINA_HOME=${CATALINA_HOME}"

if [[ ! -f "/etc/ledp/latticeengines.properties" ]]; then
    if [[ "${LE_IS_DR}" == "true" ]]; then
        PROP_ENV="prodcluster_dr"
    else
        PROP_ENV="${LE_ENVIRONMENT}"
    fi
    echo "copying properties file for PROP_ENV=${PROP_ENV}"
    cp /tmp/conf/env/${PROP_ENV}/latticeengines.properties /etc/ledp
fi

if [[ -f "/etc/ledp/ledp_keystore.jks" ]]; then
    echo "copying jks file from /etc/ledp/ledp_keystore.jks"
    cp -f /etc/ledp/ledp_keystore.jks /etc/pki/java/tomcat.jks
    cp -f /etc/ledp/cacerts /etc/pki/java/cacerts
fi
if [[ -f "/etc/ledp/lattice.crt" ]]; then
    echo "Copying /etc/ledp/lattice.crt to /etc/pki/tls/server.crt"
    cp -f /etc/ledp/lattice.crt /etc/pki/tls/server.crt
fi
if [[ -f "/etc/ledp/lattice.pem" ]]; then
    echo "Copying /etc/ledp/lattice.pem /etc/pki/tls/server.key"
    cp -f /etc/ledp/lattice.pem /etc/pki/tls/server.key
fi
chmod -R 644 /etc/pki/tls

if [[ -f "/etc/ledp/jmxtrans-agent.jar" ]]; then
    echo "Copying /etc/ledp/jmxtrans-agent.jar to /var/lib/jmxtrans-agent.jar"
    cp -f /etc/ledp/jmxtrans-agent.jar /var/lib/jmxtrans-agent.jar
    echo "Copying /etc/ledp/jmxtrans-tomcat-query.xml to ${CATALINA_HOME}/conf/jmxtrans-tomcat-query.xml"
    cp -f /etc/ledp/jmxtrans-tomcat-query.xml ${CATALINA_HOME}/conf/jmxtrans-tomcat-query.xml
fi

if [[ -f "/etc/ledp/jacocoagent.jar" ]]; then
    echo "Copying /etc/ledp/jacocoagent.jar to /var/lib/jacocoagent.jar"
    cp -f /etc/ledp/jacocoagent.jar /var/lib/jacocoagent.jar
    chmod a+x /var/lib/jacocoagent.jar
fi

# mail config
if [[ -f "/etc/init.d/postfix" ]]; then
    if [[ "${LE_ENVIRONMENT}" = "prodcluster" ]] && [[ -f "/root/postfix/main.cf.production" ]]; then
        echo "use production cf"
        cp -f /root/postfix/main.cf.production /etc/postfix/main.cf
    elif [[ -f "/root/postfix/main.cf.dev" ]]; then
        echo "use dev cf"
        cp -f /root/postfix/main.cf.dev /etc/postfix/main.cf
    fi
    /etc/init.d/postfix restart
fi


export LE_PROPDIR="/etc/ledp"

export RMI_SERVER=${RMI_SERVER:0.0.0.0}
if [[ -f "/etc/internaladdr.txt" ]]; then
    export QUARTZ_EXECUTION_HOST=`cat /etc/internaladdr.txt`
    echo "QUARTZ_EXECUTION_HOST=${QUARTZ_EXECUTION_HOST}"
    export METRIC_ADVERTISE_NAME=${HOSTNAME}-`cat /etc/internaladdr.txt | sed 's|[.]|-|g'`
    echo "METRIC_ADVERTISE_NAME=${METRIC_ADVERTISE_NAME}"
    export RMI_SERVER=`cat /etc/internaladdr.txt`
    echo "RMI_SERVER=${RMI_SERVER}"
fi

if [[ ! -z $(java -version 2>&1 |  grep "11.0") ]]; then
    echo "Java version: $(java -version 2>&1), enabling G1GC"
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseG1GC"
else
    echo "Java version: $(java -version 2>&1), enabling G1GC"
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -XX:+UseG1GC -XX:+PrintGCTimeStamps"
fi

export CATALINA_OPTS="${CATALINA_OPTS} -Dfile.encoding=UTF8 -Duser.timezone=UTC"
export CATALINA_OPTS="${CATALINA_OPTS} -Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.latticeengines.refreshScoreArtifactCache=true"
export CATALINA_OPTS="${CATALINA_OPTS} -Dio.lettuce.core.topology.sort=RANDOMIZE"

export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.authenticate=true"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.password.file=${CATALINA_HOME}/conf/jmxremote.password"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.access.file=${CATALINA_HOME}/conf/jmxremote.access"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.ssl=false"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.local.only=false"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.port=1099"
export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.rmi.port=1099"
export CATALINA_OPTS="${CATALINA_OPTS} -Djava.rmi.server.hostname=${RMI_SERVER}"

if [[ "${DISABLE_JMXTRANS}" != "true" ]]; then
    if [[ -f "/var/lib/jmxtrans-agent.jar" ]]; then
        echo "Found jmxtrans-agent.jar, setting its java agent"
        export CATALINA_OPTS="${CATALINA_OPTS} -javaagent:/var/lib/jmxtrans-agent.jar=${CATALINA_HOME}/conf/jmxtrans-tomcat-query.xml"
    else
        echo "Cannot find /var/lib/jmxtrans-agent.jar"
        exit -1
    fi
fi

if [[ "${ENABLE_JACOCO}" == "true" ]]; then
    if [[  -f "/var/lib/jacocoagent.jar" ]]; then
        JACOCO_DEST_FILE="/mnt/efs/jacoco/${HOSTNAME}.exec"
        export CATALINA_OPTS="${CATALINA_OPTS} -javaagent:/var/lib/jacocoagent.jar=destfile=${JACOCO_DEST_FILE},append=true,includes=com.latticeengines.*,jmx=true"
    else
        echo "Cannot find /var/lib/jacocoagent.jar"
        exit -1
    fi
fi

if [[ "${LE_RASP}" == "true" ]] && [[ -f "/etc/ledp/contrast.jar" ]] && [[ -f "/etc/ledp/contrast_security.yaml" ]] ; then
    export CONTRAST_CONFIG_PATH="/etc/ledp/contrast_security.yaml"
    export CATALINA_OPTS="${CATALINA_OPTS} -javaagent:/etc/ledp/contrast.jar"
fi

echo ${CATALINA_OPTS}

mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat:tomcat ${CATALINA_HOME}

if [[ "${ENABLE_JACOCO}" == "true" ]]; then
    pid=0
    export CATALINA_PID=/var/run/tomcat

    # SIGTERM-handler
    term_handler() {
      if [[ $pid -ne 0 ]]; then
        echo 'in SIGTERM handler'
        JACOCO_DEST_FILE="/mnt/efs/jacoco/${HOSTNAME}.exec"
        if [[ -f "${JACOCO_DEST_FILE}" ]]; then
            chmod a+wr ${JACOCO_DEST_FILE}
        fi
        kill -SIGTERM "$pid"
        wait "$pid"
      fi
      exit 143; # 128 + 15 -- SIGTERM
    }

    trap 'kill ${!}; term_handler' SIGTERM
    ${CATALINA_HOME}/bin/catalina.sh run &
    pid="$!"
    echo "pid=${pid}"

    # wait forever
    while true
    do
      tail -f ${CATALINA_HOME}/logs/catalina*.log & wait ${!}
    done
else
    ${CATALINA_HOME}/bin/catalina.sh run
fi


