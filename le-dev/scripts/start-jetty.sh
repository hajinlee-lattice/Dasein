#!/bin/bash

export API_PROPDIR=$WSHOME/le-api/conf/env/dev
export DATAPLATFORM_PROPDIR=$WSHOME/le-dataplatform/conf/env/dev
export DB_PROPDIR=$WSHOME/le-db/conf/env/dev
export SECURITY_PROPDIR=$WSHOME/le-security/conf/env/dev
export EAI_PROPDIR=$WSHOME/le-eai/conf/env/dev
export METADATA_PROPDIR=$WSHOME/le-metadata/conf/env/dev
export DATAFLOWAPI_PROPDIR=$WSHOME/le-dataflowapi/conf/env/dev
export PROPDATA_PROPDIR=$WSHOME/le-propdata/conf/env/dev
export MONITOR_PROPDIR=$WSHOME/le-monitor/conf/env/dev
export WORKFLOWAPI_PROPDIR=$WSHOME/le-workflowapi/conf/env/dev
export PROXY_PROPDIR=$WSHOME/le-proxy/conf/env/dev
export SCORING_PROPDIR=$WSHOME/le-scoring/conf/env/dev
export CAMILLE_PROPDIR=$WSHOME/le-camille/conf/env/dev
export DATAFLOW_PROPDIR=$WSHOME/le-dataflow/conf/env/dev
export SCORINGAPI_PROPDIR=$WSHOME/le-scoringapi/conf/env/dev
export PLS_PROPDIR=$WSHOME/le-pls/conf/env/dev
export WORKFLOW_PROPDIR=$WSHOME/le-workflow/conf/env/dev
export OAUTH2_DB_PROPDIR=$WSHOME/le-oauth2db/conf/env/dev
export MICROSERVICE_PROPDIR=$WSHOME/le-microservice/core/conf/env/dev
export TRANSFORM_PROPDIR=$WSHOME/le-transform/core/conf/env/dev
export QUARTZ_PROPDIR=$WSHOME/le-quartz/conf/env/dev
export QUARTZCLIENT_PROPDIR=$WSHOME/le-quartzclient/conf/env/dev
export JAVA_OPTIONS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=n -XX:MaxPermSize=560m -Dsqoop.throwOnError=true -Djetty.class.path=$JAVA_HOME/lib/tools.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.6.0.2.2.0.0-2041.jar:$HADOOP_HOME/etc/hadoop"
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop

pushd $JETTY_HOME
echo "Running jetty..."
java $JAVA_OPTIONS -jar $JETTY_HOME/start.jar -Djetty.port=8080 -Dcom.latticeengines.registerBootstrappers=true --lib=$HADOOP_HOME/etc/hadoop:$TEZ_CONF_DIR 2>&1 | tee /tmp/jetty.log
