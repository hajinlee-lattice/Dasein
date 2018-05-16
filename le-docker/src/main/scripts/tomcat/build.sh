#!/usr/bin/env bash

MICROSERVICES=$1

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true

source ../functions.sh

if [ ! -f "le-tomcatbase/files/jmxtrans-agent-1.2.6.jar" ]; then
    wget http://10.41.1.10/tars/jmxtrans-agent-1.2.6.jar  -O le-tomcatbase/files/jmxtrans-agent-1.2.6.jar
fi

for img in 'haproxy' 'swagger' 'tomcatbase'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

pushd le-microservice
bash build.sh $MICROSERVICES
popd

for img in 'oauth2'; do

    if [[ $MICROSERVICES == *"${img}"* ]]; then
        pushd le-${img} &&
        build_docker latticeengines/${img} &&
        popd &
    fi

done
wait

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true
