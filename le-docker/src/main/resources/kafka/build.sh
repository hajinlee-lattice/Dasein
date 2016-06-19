#!/bin/bash

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

build_docker() {
	IMAGE=$1	
	sed -i.bak "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
	docker build -t $IMAGE . || true
	mv Dockerfile.bak Dockerfile
}

pushd ../zookeeper/le-zk
build_docker latticeengines/zookeeper
popd

pushd le-kafka
build_docker latticeengines/kafka
popd

pushd le-kafka-schema-registry
build_docker latticeengines/kafka-schema-registry
popd

pushd le-kafka-rest
build_docker latticeengines/kafka-rest
popd

pushd le-kafka-haproxy
build_docker latticeengines/kafka-haproxy
popd

pushd le-kafka-manager
build_docker latticeengines/kafka-manager
popd

docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null