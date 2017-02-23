#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

build_docker() {
	IMAGE=$1
	sed -i.bak "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
	docker build -t $IMAGE . || true
	mv Dockerfile.bak Dockerfile
}

pushd le-centos7
build_docker latticeengines/centos7
popd

pushd le-ubuntu
build_docker latticeengines/ubuntu
popd

pushd le-httpd
build_docker latticeengines/httpd
popd

pushd le-centos
build_docker latticeengines/centos
popd

pushd le-jdk
bash build.sh
popd

pushd le-jre
bash build.sh
popd

pushd le-tomcat
bash build.sh
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null