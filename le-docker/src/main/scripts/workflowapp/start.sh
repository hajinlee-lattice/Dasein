#!/usr/bin/env bash
echo "JAVA_HOME=${JAVA_HOME}"

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

if [ -f "/etc/internaladdr.txt" ]; then
    export METRIC_ADVERTISE_NAME=${HOSTNAME}-`cat /etc/internaladdr.txt | sed 's|[.]|-|g'`
    echo "METRIC_ADVERTISE_NAME=${METRIC_ADVERTISE_NAME}"
fi

export JAVA_OPTS="-Duser.timezone=US/Eastern"
export JAVA_OPTS="${JAVA_OPTS} -Djavax.net.ssl.trustStore=/etc/pki/java/cacerts"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=false"
export JAVA_OPTS="${JAVA_OPTS} -Dfile.encoding=UTF8"

echo ${JAVA_OPTS}

mkdir /var/log/ledp
chmod a+w /var/log/ledp

java ${JAVA_OPTS} -classpath /workflowapp.jar com.latticeengines.workflow.core.WorkflowApp
