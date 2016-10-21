#!/usr/bin/env bash

export PYTHONPATH=$WSHOME/le-awsenvironment/src/main/python:$PYTHONPATH

IMAGE=$1

if [ -z "${IMAGE}" ]; then

    for img in 'httpd' 'zookeeper' 'observer' 'kafka' 'schema-registry' 'kafka-connect' 'tomcat' 'matchapi' 'express'
    do
        echo "pulling ${img}"
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