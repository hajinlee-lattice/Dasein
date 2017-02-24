#!/usr/bin/env bash

export PYTHONPATH=$WSHOME/le-awsenvironment/src/main/python:$PYTHONPATH

IMAGE=$1

if [ -z "${IMAGE}" ]; then

    for img in 'mysql' 'zookeeper' 'dynamo'
    do
        echo "pulling ${img}"
        python -m latticeengines.ecr.docker pull ${img}
    done

else

    python -m latticeengines.ecr.docker pull ${IMAGE}

fi

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source $WSHOME/le-dev/aliases

dkrmi