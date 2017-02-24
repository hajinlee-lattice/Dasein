#!/usr/bin/env bash

ATS_ADDRESS=${ATS_ADDRESS:=localhost}
RM_ADDRESS=${RM_ADDRESS:=localhost}

echo "Using ApplicationTimeService at ${ATS_ADDRESS}"
echo "Using ResourceManger at ${RM_ADDRESS}"

sed -i "s/{{ATS_ADDRESS}}/${ATS_ADDRESS}/g" /opt/apache-tomcat-8.5.8/webapps/tez-ui/config/configs.env
sed -i "s/{{RM_ADDRESS}}/${RM_ADDRESS}/g" /opt/apache-tomcat-8.5.8/webapps/tez-ui/config/configs.env

cat /opt/apache-tomcat-8.5.8/webapps/tez-ui/config/configs.env

export JAVA_HOME="/usr/java/jdk1.8.0_101"
/bin/bash /opt/apache-tomcat-8.5.8/bin/catalina.sh run