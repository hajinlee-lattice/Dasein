export API_PROPDIR=$HOME/workspace/ledp/le-api/conf/env/dev
export DATAPLATFORM_PROPDIR=$HOME/workspace/ledp/le-dataplatform/conf/env/dev
export DB_PROPDIR=$HOME/workspace/ledp/le-db/conf/env/dev
export SECURITY_PROPDIR=$HOME/workspace/ledp/le-security/conf/env/dev
export EAI_PROPDIR=$HOME/workspace/ledp/le-eai/conf/env/dev
export METADATA_PROPDIR=$HOME/workspace/ledp/le-metadata/conf/env/dev
export DATAFLOWAPI_PROPDIR=$HOME/workspace/ledp/le-dataflowapi/conf/env/dev
export WORKFLOWAPI_PROPDIR=$HOME/workspace/ledp/le-workflowapi/conf/env/dev
export PROPDATA_PROPDIR=$HOME/workspace/ledp/le-propdata/conf/env/dev
export MONITOR_PROPDIR=$HOME/workspace/ledp/le-monitor/conf/env/dev
export SCORING_PROPDIR=$HOME/workspace/ledp/le-scoring/conf/env/dev
export PLS_PROPDIR=$HOME/workspace/ledp/le-pls/conf/env/dev
export PROXY_PROPDIR=$HOME/workspace/ledp/le-proxy/conf/env/dev
export WORKFLOW_PROPDIR=$HOME/workspace/ledp/le-workflow/conf/env/dev
export DATAFLOW_PROPDIR=$HOME/workspace/ledp/le-dataflow/conf/env/dev
export CAMILLE_PROPDIR=$HOME/workspace/ledp/le-camille/conf/env/dev

mvn -Pfunctional -Dtest=$1 integration-test -DargLine=""
