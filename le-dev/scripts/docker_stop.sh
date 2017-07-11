#!/usr/bin/env bash

IMAGE=$1

if [ "${IMAGE}" = "tez-ui" ] || [ "${IMAGE}" = "mysql" ] || [ "${IMAGE}" = "zookeeper" ]; then

    docker run -itd --name lattice_${IMAGE} \
        -v $DOCKER_DATA_ROOT/dynamo:/var/lib/dynamo \
        -p 8000:8000 \
        latticeengines/${IMAGE}

else

    docker stop lattice_${IMAGE}

fi

dkrmc 2>/dev/null
