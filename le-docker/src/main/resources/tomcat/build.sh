#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

pushd le-tomcat
build_docker latticeengines/tomcat
popd

pushd le-matchapi
build_docker latticeengines/matchapi
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null