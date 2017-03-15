#!/usr/bin/env bash

if [ "${ANACONDA_HOME}" = "" ]; then
    echo "Must specify ANACONDA_HOME to be a non-existing folder and also the current user has the privilege to create it"
    exit 1
fi

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    UNAME=`uname`
    if [[ "${UNAME}" == 'Darwin' ]]; then
        echo "You are on Mac"
        ANACONDA_SH=Anaconda2-4.3.0-MacOSX-x86_64.sh
        NUMPY_VERSION="1.8.2"
    else
        echo "You are on ${UNAME}"
        ANACONDA_SH=Anaconda2-4.3.0-Linux-x86_64.sh
        NUMPY_VERSION="1.8.2=py27_1"
    fi

    if [ -f $ARTIFACT_DIR/$ANACONDA_SH ]; then
        echo "Skipping download of Anaconda"
    else
        echo "Downloading Anaconda"
        wget https://repo.continuum.io/archive/$ANACONDA_SH -O $ARTIFACT_DIR/$ANACONDA_SH
    fi

    if [ -d $ANACONDA_HOME ]; then
        echo "Skipping installation of Anaconda"
    else
        echo "Downloading Anaconda"
        pushd $ARTIFACT_DIR
        bash $ARTIFACT_DIR/$ANACONDA_SH -b -p $ANACONDA_HOME
        popd
    fi

    echo "Removing existing Anaconda environments"
    sudo rm -rf $ANACONDA_HOME/envs/* || true

    CONDA_ARTIFACT_DIR=$WSHOME/le-dev/conda/artifacts

    CONDAENV_LATTICE="lattice"
    echo "Creating Anaconda environment: $CONDAENV_LATTICE"
    $ANACONDA_HOME/bin/conda create -n $CONDAENV_LATTICE -y python=2.7.13 pip
    cp $CONDA_ARTIFACT_DIR/libgcrypt.so.11.8.2 $ANACONDA_HOME/envs/$CONDAENV_LATTICE/lib
    ln -s $ANACONDA_HOME/envs/$CONDAENV_LATTICE/lib/libgcrypt.so.11.8.2 $ANACONDA_HOME/envs/$CONDAENV_LATTICE/lib/libgcrypt.so.11

    CONDAENV_V01="v01"
    echo "Creating Anaconda environment: $CONDAENV_V01"
    $ANACONDA_HOME/bin/conda create -n $CONDAENV_V01 -y python=2.7.13 pip
    cp $CONDA_ARTIFACT_DIR/libgcrypt.so.11.8.2 $ANACONDA_HOME/envs/$CONDAENV_V01/lib
    ln -s $ANACONDA_HOME/envs/$CONDAENV_V01/lib/libgcrypt.so.11.8.2 $ANACONDA_HOME/envs/$CONDAENV_V01/lib/libgcrypt.so.11
fi

source $ANACONDA_HOME/bin/activate $CONDAENV_LATTICE

pip install \
    avro==1.7.7 \
    fastavro==0.7.7 \
    kazoo==2.2.1 \
    patsy==0.3.0 \
    pexpect==4.0.1 \
    psutil==2.2.1 \
    ptyprocess==0.5.1 \
    python-dateutil==2.4.1

$ANACONDA_HOME/bin/conda install -y pandas=0.13.1=np18py27_0

pip install statsmodels==0.5.0

$ANACONDA_HOME/bin/conda install -y \
    libiconv=1.14=0 \
    libxml2=2.9.4=0 \
    libxslt=1.1.28=3 \
    lxml=3.4.0=py27_0 \
    numpy=${NUMPY_VERSION} \
    openssl=1.0.2j=0 \
    py=1.4.31=py27_0 \
    pytest=2.9.2=py27_0 \
    pytz=2016.10=py27_0 \
    readline=6.2=2 \
    scikit-learn=0.14.1=np18py27_1 \
    setuptools=27.2.0=py27_0 \
    sqlite=3.13.0=0 \
    tk=8.5.18=0 \
    wheel=0.29.0=py27_0 \
    zlib=1.2.8=3

sudo mkdir -p /usr/local/bin || true
sudo ln -sf $ANACONDA_HOME/envs/$CONDAENV_LATTICE/bin/python /usr/local/bin/python2.7

source $ANACONDA_HOME/bin/deactivate


source $ANACONDA_HOME/bin/activate $CONDAENV_V01

pip install \
    avro==1.8.1 \
    fastavro==0.12.1 \
    kazoo==2.2.1 \
    patsy==0.4.1 \
    pexpect==4.2.1 \
    psutil==5.2.0 \
    python-dateutil==2.6.0

$ANACONDA_HOME/bin/conda install -y pandas=0.19.2=np112py27_1

pip install statsmodels==0.8.0


$ANACONDA_HOME/bin/conda install -y \
    libiconv=1.14=0 \
    libxml2=2.9.4=0 \
    libxslt=1.1.29=0 \
    lxml=3.7.3=py27_0 \
    numpy=1.12.0=py27_0 \
    openssl=1.0.2k=1 \
    py=1.4.32=py27_0 \
    pytest=3.0.6=py27_0 \
    pytz=2016.10=py27_0 \
    readline=6.2=2 \
    scikit-learn=0.18.1=np112py27_1 \
    setuptools=27.2.0=py27_0 \
    sqlite=3.13.0=0 \
    tk=8.5.18=0 \
    wheel=0.29.0=py27_0 \
    zlib=1.2.8=3

pip install sklearn-pandas==1.3.0
pip install git+https://github.com/jpmml/sklearn2pmml.git@0.17.4

source $ANACONDA_HOME/bin/deactivate

