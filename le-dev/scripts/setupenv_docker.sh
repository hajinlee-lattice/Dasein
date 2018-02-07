#!/bin/bash

if [ "${LE_USING_DOCKER}" == "true" ]; then

    printf "%s\n" "${DOCKER_DATA_ROOT:?You must set DOCKER_DATA_ROOT}"

    sudo mkdir -p ${DOCKER_DATA_ROOT} || true
    sudo chown -R $USER $DOCKER_DATA_ROOT

    bash $WSHOME/le-dev/scripts/docker_pull.sh
fi