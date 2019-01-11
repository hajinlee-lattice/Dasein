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
	source $ANACONDA_HOME/bin/activate v01
fi

#The following are required env variables set by caller. Here's samples for testing purpose.
export StepflowConfig="{\"inputPaths\":[\"/Pods/Aps/input/*.avro\"], \"outputPath\":\"/Pods/Aps/output\"}"
export PYTHON_APP="./apsgenerator.py"
export SHDP_HD_FSWEB='http://localhost:50070/webhdfs/v1'
##export SHDP_HD_FSWEB='http://webhdfs.lattice.local:14000/webhdfs/v1'
##export SHDP_HD_FSWEB='http://webhdfs.prod.lattice.local:14000/webhdfs/v1'

echo "python app:" $PYTHON_APP
python $PYTHON_APP
