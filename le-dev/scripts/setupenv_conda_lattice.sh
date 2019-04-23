#!/usr/bin/env bash

CONDA_ENVS_DIR=${WSHOME}//le-dev/conda/envs

if [[ -d ${ANACONDA_HOME}/envs/lattice ]]; then
            echo "Removing existing Anaconda environment: lattice"
            ${ANACONDA_HOME}/bin/conda remove -y --name lattice --all
        fi
        echo "Creating Anaconda environment: lattice"
        ${ANACONDA_HOME}/bin/conda env create -f ${CONDA_ENVS_DIR}/lattice.yaml

source ${ANACONDA_HOME}/bin/activate lattice
if [[ "$(uname)" != "Darwin" ]]; then
    ${ANACONDA_HOME}/bin/conda install -y -c clinicalgraphics libgcrypt11
    ${ANACONDA_HOME}/bin/conda install -y libgfortran=1 libgpg-error
fi
# verify
{
    python -c "import avro; print \"avro: ok\"" && \
    python -c "import numpy; print \"numpy=%s\" % numpy.__version__" && \
    python -c "import pandas; print \"pandas=%s\" % pandas.__version__" && \
    python -c "import sklearn; print \"sklearn=%s\" % sklearn.__version__" && \
    python -c "import statsmodels; print \"statsmodels=%s\" % statsmodels.__version__" && \
    python -c "from lxml import etree; print \"lxml: ok\""
} || {
    echo "Conda env was not installed successfully! Check log above"
    exit -1
}
source ${ANACONDA_HOME}/bin/deactivate
