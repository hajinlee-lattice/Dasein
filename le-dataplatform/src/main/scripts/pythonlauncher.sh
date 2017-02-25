#!/bin/bash
if [ "${ANACONDA_HOME}" = "" ]; then
    ANACONDA_HOME=/opt/conda
fi


source $ANACONDA_HOME/bin/activate $1
python2 "${@:2}"