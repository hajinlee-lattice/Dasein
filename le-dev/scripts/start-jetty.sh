#!/bin/bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"
printf "%s\n" "${HADOOP_COMMON_JAR:?You must set HADOOP_COMMON_JAR}"
printf "%s\n" "${TEZ_CONF_DIR:?You must set TEZ_CONF_DIR}"
printf "%s\n" "${JETTY_HOME:?You must set JETTY_HOME}"

export LE_PROPDIR=$WSHOME/le-config/conf/env/dev

export JAVA_OPTIONS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=n -XX:MaxPermSize=560m -Dsqoop.throwOnError=true -Djetty.class.path=$JAVA_HOME/lib/tools.jar:$HADOOP_COMMON_JAR"

pushd $JETTY_HOME
echo "Running jetty..."
java $JAVA_OPTIONS -jar $JETTY_HOME/start.jar -Djetty.port=8080 -Dcom.latticeengines.registerBootstrappers=true 2>&1 | tee /tmp/jetty.log
