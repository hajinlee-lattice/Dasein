#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

cd le-zk
build_docker latticeengines/zookeeper
cd ..

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null