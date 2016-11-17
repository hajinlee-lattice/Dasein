#!/usr/bin/env bash

export CATALINA_HOME=/usr/local/tomcat
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

if [ ! -f "/etc/ledp/latticeengines.properties" ]; then
    echo "copying properties file for LE_ENVIRONMENT=${LE_ENVIRONMENT}"
    cp /tmp/conf/env/${LE_ENVIRONMENT}/latticeengines.properties /etc/ledp
fi

export LE_PROPDIR="/etc/ledp"

if [ -f "/etc/internaladdr.txt" ]; then
    export QUARTZ_EXECUTION_HOST=`cat /etc/internaladdr.txt`
    echo "QUARTZ_EXECUTION_HOST=${QUARTZ_EXECUTION_HOST}"
fi

if [ -f "/etc/efsip.txt" ]; then
    EFS_IP=`cat /etc/efsip.txt`
    echo "EFS_IP=${EFSI_IP}"
    echo "${EFS_IP}:/ /mnt/efs nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 0 0" >> /etc/fstab
    mkdir -p /mnt/efs
    mount -a
    mkdir -p /mnt/efs/scoringapi
    chmod 777 /mnt/efs/scoringapi
    rm -rf /var/cache/scoringapi || true
    ln -s /mnt/efs/scoringapi /var/cache/scoringapi
    chmod 777 /var/cache/scoringapi
fi

export JAVA_OPTS="-Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1099"
if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi
export CATALINA_CLASSPATH=$CLASSPATH:$TEZ_CONF_DIR:$HADOOP_HOME/etc/hadoop:$JAVA_HOME/lib/tools.jar:$HADOOP_COMMON_JAR

echo ${JAVA_OPTS}

${CATALINA_HOME}/bin/catalina.sh run