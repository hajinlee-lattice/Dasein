#!/bin/bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

build_docker() {
	IMAGE=$1	
	sed -i.bak "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
	docker build -t $IMAGE . || true
	mv Dockerfile.bak Dockerfile
}

pushd le-centos
build_docker latticeengines/centos
popd

pushd le-httpd
build_docker latticeengines/httpd
popd

pushd le-ubuntu
build_docker latticeengines/ubuntu
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null