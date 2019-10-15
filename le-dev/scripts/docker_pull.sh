#!/usr/bin/env bash

export PYTHONPATH=${WSHOME}/le-dev/scripts:${PYTHONPATH}

IMAGE=$1

if [[ -z "${IMAGE}" ]]; then

    for img in 'mysql' 'zookeeper' 'redis' 'tez-ui'
    do
        echo "pulling ${img}"
        python -m docker pull ${img}
    done

else

    python -m docker pull ${IMAGE}

fi

# Expand aliases
echo "Expanding aliases."
shopt -s expand_aliases
echo "Sourcing aliases file"
source ${WSHOME}/le-dev/aliases

dkrmi
