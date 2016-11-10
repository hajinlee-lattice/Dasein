#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

pushd le-discover
build_docker latticeengines/discover
popd

cd le-zk
build_docker latticeengines/zookeeper
cd ..

cd le-observer
build_docker latticeengines/observer
cd ..

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null