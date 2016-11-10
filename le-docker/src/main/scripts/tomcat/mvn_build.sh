#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

for img in 'haproxy' 'config' 'swagger' 'tomcat'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

for img in 'matchapi' 'scoringapi' 'oauth2'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

for img in 'playmaker' 'admin' 'pls'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

pushd le-microservice
bash mvn_build.sh
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null