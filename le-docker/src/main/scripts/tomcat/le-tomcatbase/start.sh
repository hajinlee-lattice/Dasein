#!/usr/bin/env bash
echo "JAVA_HOME=${JAVA_HOME}"
echo "CATALINA_HOME=${CATALINA_HOME}"

if [ ! -f "/etc/ledp/latticeengines.properties" ]; then
    echo "copying properties file for LE_ENVIRONMENT=${LE_ENVIRONMENT}"
    cp /tmp/conf/env/${LE_ENVIRONMENT}/latticeengines.properties /etc/ledp
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
    export JAVA_OPTS="${JAVA_OPTS} -javaagent:/var/lib/jacocoagent.jar=includes=com.latticeengines.*,destFile=${JACOCO_DEST_FILE}"
fi

if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi
export CATALINA_CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR

echo ${JAVA_OPTS}

mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat ${CATALINA_HOME}
${CATALINA_HOME}/bin/catalina.sh run
