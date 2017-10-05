#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

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

pushd le-centos7
build_docker latticeengines/centos7 ${NO_CACHE}
popd

pushd le-ubuntu
build_docker latticeengines/ubuntu ${NO_CACHE}
popd

pushd le-python
build_docker latticeengines/python ${NO_CACHE}
popd

pushd le-httpd
build_docker latticeengines/httpd ${NO_CACHE}
popd

pushd le-centos
build_docker latticeengines/centos ${NO_CACHE}
popd

pushd le-jdk
build_docker latticeengines/jdk ${NO_CACHE}
docker tag latticeengines/jdk:latest latticeengines/jdk:1.8
popd

pushd le-jre
build_docker latticeengines/jre ${NO_CACHE}
docker tag latticeengines/jre:latest latticeengines/jre:1.8
popd

pushd le-tomcat
build_docker latticeengines/tomcat ${NO_CACHE}
docker tag latticeengines/tomcat:latest latticeengines/tomcat:8.5
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null