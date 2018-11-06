#!/usr/bin/env bash

source ../functions.sh

for img in 'haproxy' 'swagger'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} &&
    popd &

done
wait

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true
