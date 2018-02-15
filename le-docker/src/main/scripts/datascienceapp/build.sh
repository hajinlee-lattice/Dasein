#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

build_docker latticeengines/datascienceapp

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null
