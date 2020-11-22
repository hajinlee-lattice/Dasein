#!/usr/bin/env bash

MICROSERVICES=$1

DKRMI_FLAG="${DKRMI_FLAG:=true}"
($DKRMI_FLAG) && docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true

source ../functions.sh

pushd le-tomcatbase
build_docker latticeengines/tomcatbase
popd

if [[ ${MICROSERVICES} == *"saml"* ]] || [[ ${MICROSERVICES} == *"pls"* ]] || [[ ${MICROSERVICES} == *"eai"* ]]; then
    pushd le-tomcatbase &&
    build_docker2 latticeengines/tomcatbase-j8 &&
    popd
fi

pushd le-microservice
bash build.sh $MICROSERVICES
popd

for img in 'oauth2'; do

    if [[ $MICROSERVICES == *"${img}"* ]]; then
        pushd le-${img} &&
        build_docker latticeengines/${img} &&
        popd &
    fi

done
wait

($DKRMI_FLAG) && docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true