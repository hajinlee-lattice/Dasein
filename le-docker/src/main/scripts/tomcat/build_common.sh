#!/usr/bin/env bash

build_docker() {
	IMAGE=$1
    NO_CACHE=$2
    sed -i.bak "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
    if [ "${NO_CACHE}" == "--no-cache" ]; then
	    docker build --no-cache -t ${IMAGE} . || true
	else
	    docker build -t ${IMAGE} . || true
	fi
	mv Dockerfile.bak Dockerfile
}

NO_CACHE=$1

for img in 'haproxy' 'swagger'; do

    pushd le-${img} &&
    build_docker latticeengines/${img} ${NO_CACHE} &&
    popd &

done
wait

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null || true
