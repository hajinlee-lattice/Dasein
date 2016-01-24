#!/bin/bash

export API_PROPDIR=$PWD/../le-api/conf/env/dev
export DATAPLATFORM_PROPDIR=$PWD/../le-dataplatform/conf/env/dev
export DB_PROPDIR=$PWD/../le-db/conf/env/dev
export SECURITY_PROPDIR=$PWD/../le-security/conf/env/dev
export EAI_PROPDIR=$PWD/../le-eai/conf/env/dev
export METADATA_PROPDIR=$PWD/../le-metadata/conf/env/dev
export DATAFLOWAPI_PROPDIR=$PWD/../le-dataflowapi/conf/env/dev
export PROPDATA_PROPDIR=$PWD/../le-propdata/conf/env/dev
export MONITOR_PROPDIR=$PWD/../le-monitor/conf/env/dev
export WORKFLOWAPI_PROPDIR=$PWD/../le-workflowapi/conf/env/dev
export PROXY_PROPDIR=$PWD/../le-proxy/conf/env/dev
export SCORING_PROPDIR=$PWD/../le-scoring/conf/env/dev
export CAMILLE_PROPDIR=$PWD/../le-camille/conf/env/dev
export DATAFLOW_PROPDIR=$PWD/../le-dataflow/conf/env/dev
export JAVA_OPTIONS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4005,server=y,suspend=n -XX:MaxPermSize=1024m -Dsqoop.throwOnError=true -Djetty.class.path=$JAVA_HOME/lib/tools.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.6.0.2.2.0.0-2041.jar:$HADOOP_HOME/etc/hadoop"
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop

pushd $JETTY_HOME
echo "Running jetty..."
java $JAVA_OPTIONS -jar $JETTY_HOME/start.jar -Djetty.port=8080 --lib=$HADOOP_HOME/etc/hadoop 2>&1 | tee /tmp/jetty.log 
