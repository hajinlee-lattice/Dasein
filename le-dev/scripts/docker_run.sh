#!/usr/bin/env bash

IMAGE=$1

if [ "${IMAGE}" = "dynamo" ]; then

    docker run -itd --name lattice_${IMAGE} \
        -v $DOCKER_DATA_ROOT/dynamo:/var/lib/dynamo \
        -p 8000:8000 \
        latticeengines/${IMAGE}

else
    echo "Unknown image ${IMAGE}"
    exit -1
fi
