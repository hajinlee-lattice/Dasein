#!/usr/bin/env bash

MICROSERVICES=$1

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true

source ../functions.sh

for img in 'haproxy' 'swagger' 'tomcatbase'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

pushd le-microservice
bash build.sh $MICROSERVICES
popd

for img in 'playmaker' 'oauth2'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true