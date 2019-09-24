#!/usr/bin/env bash

IMAGE=$1

if [ "${IMAGE}" = "tez-ui" ] || [ "${IMAGE}" = "mysql" ] || [ "${IMAGE}" = "zookeeper" ]; then

    docker run -itd --name lattice_${IMAGE} \
        -v $DOCKER_DATA_ROOT/dynamo:/var/lib/dynamo \
        -p 8000:8000 \
        latticeengines/${IMAGE}

elif [ "${IMAGE}" = "kafka" ]; then

    docker-compose -p kafka -f $WSHOME/le-dev/scripts/kafka-docker-compose.yml down

else

    docker stop le_${IMAGE}

fi

dkrmc 2>/dev/null
