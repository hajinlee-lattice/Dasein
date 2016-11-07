#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

pushd le-tomcat
build_docker latticeengines/tomcat
popd

pushd le-matchapi
build_docker latticeengines/matchapi
popd

pushd le-oauth2
build_docker latticeengines/oauth2
popd

pushd le-playmaker
build_docker latticeengines/playmaker
popd

pushd le-scoringapi
build_docker latticeengines/scoringapi
popd

pushd le-microservice
bash build.sh
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null