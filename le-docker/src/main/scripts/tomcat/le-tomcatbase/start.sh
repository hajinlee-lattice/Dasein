#!/usr/bin/env bash
echo "JAVA_HOME=${JAVA_HOME}"
echo "CATALINA_HOME=${CATALINA_HOME}"

if [ ! -f "/etc/ledp/latticeengines.properties" ]; then
    echo "copying properties file for LE_ENVIRONMENT=${LE_ENVIRONMENT}"
    cp /tmp/conf/env/${LE_ENVIRONMENT}/latticeengines.properties /etc/ledp
fi

if [ -f "/etc/ledp/ledp_keystore.jks" ]; then
    echo "copying jks file from /etc/ledp/ledp_keystore.jks"
    cp -f /etc/ledp/ledp_keystore.jks /etc/pki/java/tomcat.jks
    chmod 600 /etc/pki/java/tomcat.jks
    cp -f /etc/ledp/cacerts /etc/pki/java/cacerts
    chmod 600 /etc/pki/java/cacerts
fi

# mail config
if [ "${LE_ENVIRONMENT}" = "prodcluster" ] && [ -f "/root/postfix/main.cf.production" ]; then
    echo "use production cf"
    cp -f /root/postfix/main.cf.production /etc/postfix/main.cf
elif [ -f "/root/postfix/main.cf.dev" ]; then
    echo "use dev cf"
    cp -f /root/postfix/main.cf.dev /etc/postfix/main.cf
fi
/etc/init.d/postfix restart

export LE_PROPDIR="/etc/ledp"

export RMI_SERVER="127.0.0.1"
if [ -f "/etc/internaladdr.txt" ]; then
    export QUARTZ_EXECUTION_HOST=`cat /etc/internaladdr.txt`
    echo "QUARTZ_EXECUTION_HOST=${QUARTZ_EXECUTION_HOST}"
    export METRIC_ADVERTISE_NAME=${HOSTNAME}-`cat /etc/internaladdr.txt | sed 's|[.]|-|g'`
    echo "METRIC_ADVERTISE_NAME=${METRIC_ADVERTISE_NAME}"
    export RMI_SERVER=`cat /etc/internaladdr.txt`
    echo "RMI_SERVER=${RMI_SERVER}"
fi

export JAVA_OPTS="-Duser.timezone=US/Eastern"
export JAVA_OPTS="${JAVA_OPTS} -Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.refreshScoreArtifactCache=true"
export JAVA_OPTS="${JAVA_OPTS} -Dfile.encoding=UTF8"

export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.ssl=false"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.local.only=false"
export JAVA_OPTS="${JAVA_OPTS} -Djava.rmi.server.hostname=${RMI_SERVER}"

if [ "${ENABLE_JACOCO}" == "true" ]; then
    JACOCO_DEST_FILE="/mnt/efs/jacoco/${HOSTNAME}.exec"
    export JAVA_OPTS="${JAVA_OPTS} -javaagent:/var/lib/jacocoagent.jar=destfile=${JACOCO_DEST_FILE},append=true,includes=com.latticeengines.*,jmx=true"
fi

if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi
export CATALINA_CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR

echo ${JAVA_OPTS}

mkdir /var/log/ledp
chmod a+w /var/log/ledp

chmod +x /var/lib/jacocoagent.jar
chown -R tomcat ${CATALINA_HOME}

if [ "${ENABLE_JACOCO}" == "true" ]; then
    pid=0
    export CATALINA_PID=/var/run/tomcat

    # SIGTERM-handler
    term_handler() {
      if [ $pid -ne 0 ]; then
        echo 'in SIGTERM handler'
        JACOCO_DEST_FILE="/mnt/efs/jacoco/${HOSTNAME}.exec"
        if [ -f "${JACOCO_DEST_FILE}" ]; then
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


