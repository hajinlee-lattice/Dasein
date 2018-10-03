#!/usr/bin/env bash

# passing in parameter "prod" to build image for prod environment

source ../../functions.sh

ENV=$1
ENV=${ENV:=qa}

echo ${ENV}

cp -f latticeengines.properties.${ENV} webapps/latticeengines.properties
build_docker latticeengines/sqoop

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true

