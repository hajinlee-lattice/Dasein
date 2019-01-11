#!/bin/bash
echo "start.sh:"

#The following are required env variables set by caller. Here's samples for testing purpose.
#export CONDA_ENV=v01

echo "  * Anaconda env: ${CONDA_ENV}"

if [ "${ANACONDA_HOME}" = "" ]; then
    ANACONDA_HOME=/opt/conda
fi
echo "  * ANACONDA_HOME: ${ANACONDA_HOME}"

if [ -n "${CONDA_ENV}" ]; then
	source $ANACONDA_HOME/bin/activate ${CONDA_ENV}
else
	source $ANACONDA_HOME/bin/activate lattice
fi

#The following are required env variables set by caller. Here's samples for testing purpose.

export SHDP_HD_FSWEB='http://10.141.11.185:50070/webhdfs/v1'
export PYTHON_APP_CONFIG='{"inputPaths":["/modeling-job/6e51dc2b-be13-49d1-a676-d94d7a98ef5d/*","/app/b/4.14.0-SNAPSHOT/dataplatform/scripts/*","/app/b/4.14.0-SNAPSHOT/dataplatform/scripts/leframework.tar.gz","/user/s-analytics/customers/INTERNAL_DellAPJDeploymentTestNG/data/Play_11_TrainingSample_WithRevenue/samples/s0Training-r-00000.avro","/user/s-analytics/customers/INTERNAL_DellAPJDeploymentTestNG/data/Play_11_TrainingSample_WithRevenue/samples/s0Test-r-00000.avro","/app/b/4.14.0-SNAPSHOT/dataplatform/scripts/algorithm/data_profile.py","/app/b/4.14.0-SNAPSHOT/dataplatform/scripts/pipeline.py","/app/b/4.14.0-SNAPSHOT/dataplatform/scripts/lepipeline.tar.gz","/user/s-analytics/customers/INTERNAL_DellAPJDeploymentTestNG/data/Play_11_TrainingSample_WithRevenue/Play_11_TrainingSample_WithRevenue.avsc","/user/s-analytics/customers/INTERNAL_DellAPJDeploymentTestNG/data/Play_11_EventMetadata","/user/s-analytics/customers/INTERNAL_DellAPJDeploymentTestNG/data/Play_11_EventMetadata","/app/b/4.14.0-SNAPSHOT/dataplatform/scripts/pipeline.json"],"outputPath":null,"job_id":null}'
export PYTHONIOENCODING='UTF-8'
export CONDA_ENV='UTF-8'
export PYTHON_APP='app.py'
export PYTHON_APP_ARGS='metadata.json runtimeconfig.properties'
export DEBUG='false'
export PYTHON_APP_LAUNCH='launcher.py'


echo "python app:" $PYTHON_APP
python $PYTHON_APP
