#!/usr/bin/env bash
docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true

source ../functions.sh

pushd le-express
build_docker latticeengines/express
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true
