#!/bin/bash
echo "start.sh"

#The following are required env variables set by caller. Here's samples for testing purpose.
#export CONDA_ENV=v01

echo "  * Anaconda env: ${CONDA_ENV}"

if [[ "${ANACONDA_HOME}" = "" ]]; then
    ANACONDA_HOME=/opt/conda
fi
echo "  * ANACONDA_HOME: ${ANACONDA_HOME}"

envname=v01
if [[ -n "${CONDA_ENV}" ]]; then
	envname=${CONDA_ENV}
fi
source $ANACONDA_HOME/bin/activate $envname

cp /python_app/libgcrypt.so.11.8.2 ${ANACONDA_HOME}/envs/$envname/lib
ln -s ${ANACONDA_HOME}/envs/$envname/lib/libgcrypt.so.11.8.2 ${ANACONDA_HOME}/envs/$envname/lib/libgcrypt.so.11
 
echo "python app:" $PYTHON_APP
python $PYTHON_APP
