#!/usr/bin/env bash

DKRMI_FLAG="${DKRMI_FLAG:=true}"
($DKRMI_FLAG) && docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true

source ../functions.sh

pushd le-express
build_docker latticeengines/express
popd

($DKRMI_FLAG) && docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true
