#!/usr/bin/env bash

IMAGE=$1

if [ "${IMAGE}" = "kafka" ]; then

    docker-compose -p kafka -f $WSHOME/le-dev/scripts/kafka-docker-compose.yml down

else

    if [[ "$(docker ps | grep -q le_${IMAGE})" ]]; then
      docker stop le_${IMAGE}
    fi
    docker rm "$(docker ps -a -q)" && docker ps -a 2>/dev/null

fi

dkrmc 2>/dev/null
