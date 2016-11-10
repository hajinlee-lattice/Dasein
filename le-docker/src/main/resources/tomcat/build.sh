#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

pushd le-config
build_docker latticeengines/config
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null