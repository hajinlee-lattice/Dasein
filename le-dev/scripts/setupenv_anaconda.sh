#!/usr/bin/env bash

if [[ "${ANACONDA_HOME}" = "" ]]; then
    echo "Must specify ANACONDA_HOME to be a non-existing folder and also the current user has the privilege to create it"
    exit 1
fi

BOOTSTRAP_MODE=$1
CONDA_ARTIFACT_DIR=${WSHOME}/le-dev/conda/artifacts

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    ARTIFACT_DIR=${WSHOME}/le-dev/artifacts
    MINICONDA_VERSION=4.6.14

    UNAME=`uname`
    if [[ "${UNAME}" == 'Darwin' ]]; then
        echo "You are on Mac"
        ANACONDA_SH=Miniconda3-${MINICONDA_VERSION}-MacOSX-x86_64.sh
    else
        echo "You are on ${UNAME}"
        ANACONDA_SH=Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh
    fi

    if [[ -f $ARTIFACT_DIR/$ANACONDA_SH ]]; then
        echo "Skipping download of Miniconda"
    else
        echo "Downloading Miniconda"
        wget https://repo.anaconda.com/miniconda/$ANACONDA_SH -O $ARTIFACT_DIR/$ANACONDA_SH
    fi

    echo "Delete anaconda home, because installation script will create it"
    sudo rm -rf ${ANACONDA_HOME} || true
    echo "Downloading Anaconda"
    pushd $ARTIFACT_DIR
    sudo bash $ARTIFACT_DIR/$ANACONDA_SH -b -p ${ANACONDA_HOME}
    popd
    sudo chown -R ${USER} ${ANACONDA_HOME}

    ${ANACONDA_HOME}/bin/conda config --add channels conda-forge

    ${ANACONDA_HOME}/bin/pip install --upgrade pip
    ${ANACONDA_HOME}/bin/pip install -r $WSHOME/le-dev/scripts/requirements3.txt

    if [[ "${ANACONDA_HOME}" != "/opt/conda" ]]; then
        sudo ln -f -s ${ANACONDA_HOME} /opt/conda
    fi
fi

${ANACONDA_HOME}/bin/conda update -n base -c defaults conda

CONDA_ENVS_DIR=${WSHOME}//le-dev/conda/envs
for envname in 'p2' 'leds'
    do
        if [[ -d ${ANACONDA_HOME}/envs/$envname ]]; then
            echo "Removing existing Anaconda environment: $envname"
            ${ANACONDA_HOME}/bin/conda remove -y --name $envname --all
        fi
        echo "Creating Anaconda environment: $envname"
        ${ANACONDA_HOME}/bin/conda env create -f ${CONDA_ENVS_DIR}/${envname}.yaml
    done

source ${ANACONDA_HOME}/bin/activate p2
pip install -r $WSHOME/le-dev/scripts/requirements.txt
source ${ANACONDA_HOME}/bin/deactivate

source ${ANACONDA_HOME}/bin/activate leds
jupyter nbextension enable --py --sys-prefix widgetsnbextension
jupyter-kernelspec install --user ${ANACONDA_HOME}/envs/leds/lib/python3.7/site-packages/sparkmagic/kernels/sparkkernel
jupyter-kernelspec install --user ${ANACONDA_HOME}/envs/leds/lib/python3.7/site-packages/sparkmagic/kernels/pysparkkernel
source ${ANACONDA_HOME}/bin/deactivate
