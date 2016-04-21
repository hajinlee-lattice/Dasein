#!/bin/bash

export API_PROPDIR=$WSHOME/le-api/conf/env/dev
export DATAPLATFORM_PROPDIR=$WSHOME/le-dataplatform/conf/env/dev
export DB_PROPDIR=$WSHOME/le-db/conf/env/dev
export SECURITY_PROPDIR=$WSHOME/le-security/conf/env/dev
export EAI_PROPDIR=$WSHOME/le-eai/conf/env/dev
export METADATA_PROPDIR=$WSHOME/le-metadata/conf/env/dev
export DATAFLOWAPI_PROPDIR=$WSHOME/le-dataflowapi/conf/env/dev
export WORKFLOWAPI_PROPDIR=$WSHOME/le-workflowapi/conf/env/dev
export PROPDATA_PROPDIR=$WSHOME/le-propdata/conf/env/dev
export MONITOR_PROPDIR=$WSHOME/le-monitor/conf/env/dev
export SCORING_PROPDIR=$WSHOME/le-scoring/conf/env/dev
export PLS_PROPDIR=$WSHOME/le-pls/conf/env/dev
export PROXY_PROPDIR=$WSHOME/le-proxy/conf/env/dev
export WORKFLOW_PROPDIR=$WSHOME/le-workflow/conf/env/dev
export DATAFLOW_PROPDIR=$WSHOME/le-dataflow/conf/env/dev
export CAMILLE_PROPDIR=$WSHOME/le-camille/conf/env/dev
export SCORINGAPI_PROPDIR=$WSHOME/le-scoringapi/conf/env/dev
export PLAYMAKER_PROPDIR=$WSHOME/le-playmaker/conf/env/dev
export OAUTH2_DB_PROPDIR=$WSHOME/le-oauth2db/conf/env/dev
export TRANSFORM_PROPDIR=$WSHOME/le-transform/conf/env/dev

PRODUCT=$1
TEST_TYPE=$2
TEST=$3
TESTGROUP=$2
if [ $TEST_TYPE = "deployment" ]; then
    TESTGROUP=$TEST_TYPE.$PRODUCT
fi

mvn -P$TEST_TYPE -Dtest=$TEST -D$TEST_TYPE.groups=$TESTGROUP verify -DargLine=""
