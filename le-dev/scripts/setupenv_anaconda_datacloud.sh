#!/usr/bin/env bash

if [ -z "${WSHOME}" ]; then
    echo "You must specify WSHOME, and check out the codebase there."
    exit -1
fi

if [ ! -f "${ANACONDA_HOME}/bin/conda" ]; then
    echo "Did you install anaconda?"
    exit -1
fi

CONDA_ARTIFACT_DIR=$WSHOME/le-dev/conda/artifacts

ENV_NAME='datacloud'
PY_VERSION='2.7.13'

if [ -d "${ANACONDA_HOME}/envs/${ENV_NAME}" ]; then
    echo "Removing existing Anaconda environment: ${ENV_NAME}"
    ${ANACONDA_HOME}/bin/conda remove -y --name ${ENV_NAME} --all
fi

echo "Creating Anaconda environment: ${ENV_NAME}"
${ANACONDA_HOME}/bin/conda create -n ${ENV_NAME} -y python=${PY_VERSION} pip

source ${ANACONDA_HOME}/bin/activate ${ENV_NAME}

conda install -y pymssql pycrypto

conda install -y -c bioconda mysqlclient

pip install argparse pytest pytest-pythonpath

source ${ANACONDA_HOME}/bin/deactivate ${ENV_NAME}

ln -s ${WSHOME}/le-datacloud/python/bin/datacloud ${ANACONDA_HOME}/envs/${ENV_NAME}/bin/datacloud
