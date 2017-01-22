#!/bin/bash

pushd tomcat; bash build.sh; popd
pushd nodejs; bash build.sh; popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null