#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

source ../functions.sh

mkdir -p conf

if [ ! -a ./conf/latticeengines.properties ]; then
    cp ../../../../../le-config/conf/env/dev/latticeengines.properties ./conf
fi

build_docker latticeengines/datascienceapp
