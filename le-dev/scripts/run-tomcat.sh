#!/usr/bin/env bash

export ADMIN_PROPDIR=$WSHOME/le-admin/conf/env/dev
export API_PROPDIR=$WSHOME/le-api/conf/env/dev
export DATAPLATFORM_PROPDIR=$WSHOME/le-dataplatform/conf/env/dev
export DB_PROPDIR=$WSHOME/le-db/conf/env/dev
export SECURITY_PROPDIR=$WSHOME/le-security/conf/env/dev
export EAI_PROPDIR=$WSHOME/le-eai/conf/env/dev
export METADATA_PROPDIR=$WSHOME/le-metadata/conf/env/dev
export MICROSERVICE_PROPDIR=$WSHOME/le-microservice/core/conf/env/dev
export DATAFLOWAPI_PROPDIR=$WSHOME/le-dataflowapi/conf/env/dev
export PROPDATA_PROPDIR=$WSHOME/le-propdata/conf/env/dev
export MONITOR_PROPDIR=$WSHOME/le-monitor/conf/env/dev
export WORKFLOWAPI_PROPDIR=$WSHOME/le-workflowapi/conf/env/dev
export PROXY_PROPDIR=$WSHOME/le-proxy/conf/env/dev
export SCORING_PROPDIR=$WSHOME/le-scoring/conf/env/dev
export CAMILLE_PROPDIR=$WSHOME/le-camille/conf/env/dev
export DATAFLOW_PROPDIR=$WSHOME/le-dataflow/conf/env/dev
export PLS_PROPDIR=$WSHOME/le-pls/conf/env/dev
export WORKFLOW_PROPDIR=$WSHOME/le-workflow/conf/env/dev
export PROPDATA_PROPDIR=$WSHOME/le-propdata/conf/env/dev
export JOB_PROPDIR=$WSHOME/le-job/conf/env/dev
export DELLEBI_PROPDIR=$WSHOME/le-dellebi/conf/env/dev
export TRANSFORM_PROPDIR=$WSHOME/le-transform/conf/env/dev
export SCORINGAPI_PROPDIR=$WSHOME/le-scoringapi/conf/env/dev

export JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=n"
export JAVA_OPTS="${JAVA_OPTS} -Dsqoop.throwOnError=true -XX:MaxPermSize=1g -Xmx4g"
export JAVA_OPTS="${JAVA_OPTS} -Djavax.net.ssl.trustStore=${WSHOME}/le-security/certificates/laca-ldap.dev.lattice.local.jks"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.latticeengines.registerBootstrappers=true"
export JAVA_OPTS="${JAVA_OPTS} -DLOCAL_MODEL_DL_QUARTZ_ENABLED=enabled"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=1098"
export CATALINA_CLASSPATH=$CLASSPATH:$TEZ_CONF_DIR:$HADOOP_HOME/etc/hadoop:$JAVA_HOME/lib/tools.jar:$HADOOP_COMMON_JAR

$CATALINA_HOME/bin/catalina.sh run