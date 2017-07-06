#!/usr/bin/env bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

build_docker() {
	IMAGE=$1
	sed -i.bak "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
	docker build -t $IMAGE . || true
	mv Dockerfile.bak Dockerfile
}


#pushd le-tomcat-dev
#build_docker latticeengines/tomcat-dev
#popd
#
#pushd le-tez-ui
#build_docker latticeengines/tez-ui
#popd
#
#pushd le-dynamo
#build_docker latticeengines/dynamo
#popd

pushd le-mysql
build_docker latticeengines/mysql
popd

#pushd le-zookeeper
#build_docker latticeengines/zookeeper
#popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null