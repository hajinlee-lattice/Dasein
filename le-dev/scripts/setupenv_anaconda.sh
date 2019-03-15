#!/usr/bin/env bash

if [[ "${ANACONDA_HOME}" = "" ]]; then
    echo "Must specify ANACONDA_HOME to be a non-existing folder and also the current user has the privilege to create it"
    exit 1
fi

BOOTSTRAP_MODE=$1
CONDA_ARTIFACT_DIR=${WSHOME}/le-dev/conda/artifacts

if [[ "${BOOTSTRAP_MODE}" = "bootstrap" ]]; then
    ARTIFACT_DIR=${WSHOME}/le-dev/artifacts
    ANACONDA_VERSION=2018.12

    UNAME=`uname`
    if [[ "${UNAME}" == 'Darwin' ]]; then
        echo "You are on Mac"
        ANACONDA_SH=Anaconda3-${ANACONDA_VERSION}-MacOSX-x86_64.sh
    else
        echo "You are on ${UNAME}"
        ANACONDA_SH=Anaconda3-${ANACONDA_VERSION}-Linux-x86_64.sh
    fi

    if [[ -f $ARTIFACT_DIR/$ANACONDA_SH ]]; then
        echo "Skipping download of Anaconda"
    else
        echo "Downloading Anaconda"
        wget https://repo.continuum.io/archive/$ANACONDA_SH -O $ARTIFACT_DIR/$ANACONDA_SH
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

for CONDAENV in 'lattice|2.7' 'v01|2.7' 'p2|2.7' 'spark|3.7'
    do
        envname=`echo $CONDAENV | cut -d \| -f 1`
        pythonversion=`echo $CONDAENV | cut -d \| -f 2`
        if [[ -d ${ANACONDA_HOME}/envs/$envname ]]; then
            echo "Removing existing Anaconda environment: $envname"
            ${ANACONDA_HOME}/bin/conda remove -y --name $envname --all
        fi
        echo "Creating Anaconda environment: $envname"
        ${ANACONDA_HOME}/bin/conda create -n $envname -y python=$pythonversion pip
    done

${ANACONDA_HOME}/bin/conda update -y -n base conda

source ${ANACONDA_HOME}/bin/activate p2

pip install --upgrade pip
pip install -r $WSHOME/le-dev/scripts/requirements.txt

source ${ANACONDA_HOME}/bin/deactivate

source ${ANACONDA_HOME}/bin/activate lattice

pip install --upgrade pip

pip install \
    avro \
    pexpect==4.0.1 \
    ptyprocess==0.5.1

pip install --no-deps \
    kazoo==2.2.1 \
    patsy==0.3.0

${ANACONDA_HOME}/bin/conda install -y \
    fastavro \
    lxml \
    py \
    pytest \
    pytz \
    python-snappy \
    python-dateutil \
    numpy=1.8 \
    pandas=0.13 \
    scikit-learn=0.14 \
    statsmodels=0.5

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

source ${ANACONDA_HOME}/bin/activate v01

pip install --upgrade pip

pip install \
    avro \
    pexpect==4.2.1 \
    ptyprocess==0.5.1

pip install --no-deps kazoo==2.2.1 patsy==0.4.1

${ANACONDA_HOME}/bin/conda install -y \
    fastavro \
    lxml \
    psutil \
    py \
    pytest \
    pytz \
    python-snappy \
    numpy=1.12 \
    pandas=0.19 \
    scikit-learn=0.18 \
    statsmodels=0.8

if [[ "$(uname)" != "Darwin" ]]; then
    ${ANACONDA_HOME}/bin/conda install -y -c clinicalgraphics libgcrypt11
    ${ANACONDA_HOME}/bin/conda install -y libgpg-error
fi

pip install sklearn-pandas==1.3.0
pip install git+https://github.com/jpmml/sklearn2pmml.git@0.17.4

# verify
{
    python -c "import avro; print \"avro: ok\"" && \
    python -c "import numpy; print \"numpy=%s\" % numpy.__version__" && \
    python -c "import pandas; print \"pandas=%s\" % pandas.__version__" && \
    python -c "import sklearn; print \"sklearn=%s\" % sklearn.__version__" && \
    python -c "import statsmodels; print \"statsmodels=%s\" % statsmodels.__version__" && \
    python -c "import sklearn_pandas as spd; print \"sklearn_pandas=%s\" % spd.__version__" && \
    python -c "from lxml import etree; print \"lxml: ok\""
} || {
    echo "Conda env was not installed successfully! Check log above"
    exit -1
}

source ${ANACONDA_HOME}/bin/deactivate

source ${ANACONDA_HOME}/bin/activate spark

pip install --upgrade pip

${ANACONDA_HOME}/bin/conda install -y \
	scikit-learn \
	statsmodels \
	fastavro \
	seaborn \
	scipy \
	jupyter \
	matplotlib \
	sparkmagic \
	jupyter_contrib_nbextensions \
	jupyter_nbextensions_configurator \
	ipython \
	ipykernel=4.9.0 \
	prompt_toolkit \
	pyyaml \
	jinja2

jupyter nbextension enable --py --sys-prefix widgetsnbextension
jupyter-kernelspec install --user ${ANACONDA_HOME}/envs/spark/lib/python3.5/site-packages/sparkmagic/kernels/sparkkernel
jupyter-kernelspec install --user ${ANACONDA_HOME}/envs/spark/lib/python3.5/site-packages/sparkmagic/kernels/pysparkkernel

source ${ANACONDA_HOME}/bin/deactivate
