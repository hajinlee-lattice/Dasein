#!/usr/bin/env bash

service rngd restart
service snmpd restart
service ntpd restart
service sshd restart

export CATALINA_HOME=/opt/apache-tomcat-8.5.8
export JAVA_HOME=/usr/java/default

if [ ! -f "/etc/ledp/latticeengines.properties" ]; then
    echo "copying properties file for LE_ENVIRONMENT=${LE_ENVIRONMENT}"
    cp /tmp/conf/env/${LE_ENVIRONMENT}/latticeengines.properties /etc/ledp
fi

# mail config
if [ "${LE_ENVIRONMENT}" = "prodcluster" ]; then
    echo "use production cf"
    cp -f /root/postfix/main.cf.production /etc/postfix/main.cf
else
    echo "use dev cf"
    cp -f /root/postfix/main.cf.dev /etc/postfix/main.cf
fi
/etc/init.d/postfix restart

export LE_PROPDIR="/etc/ledp"

if [ -f "/etc/internaladdr.txt" ]; then
    export QUARTZ_EXECUTION_HOST=`cat /etc/internaladdr.txt`
    echo "QUARTZ_EXECUTION_HOST=${QUARTZ_EXECUTION_HOST}"
    export METRIC_ADVERTISE_NAME=${HOSTNAME}-`cat /etc/internaladdr.txt | sed 's|[.]|-|g'`
    echo "METRIC_ADVERTISE_NAME=${METRIC_ADVERTISE_NAME}"
fi

if [ -f "/etc/efsip.txt" ]; then
    EFS_IP=`cat /etc/efsip.txt`
    echo "EFS_IP=${EFS_IP}"
    echo "${EFS_IP}:/ /mnt/efs nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 0 0" >> /etc/fstab
    mkdir -p /mnt/efs
    mount -a
    mkdir -p /mnt/efs/scoringapi
    chmod 777 /mnt/efs/scoringapi
    rm -rf /var/cache/scoringapi || true
    ln -s /mnt/efs/scoringapi /var/cache/scoringapi
    chmod 777 /var/cache/scoringapi
fi

export JAVA_OPTS="-Duser.timezone=US/Eastern -Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1099"
if [ ! -z "${CATALINA_OPTS}" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${CATALINA_OPTS}"
fi
export CATALINA_CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR

echo ${JAVA_OPTS}

mkdir /var/log/ledp
chmod a+w /var/log/ledp

chown -R tomcat ${CATALINA_HOME}
${CATALINA_HOME}/bin/catalina.sh run