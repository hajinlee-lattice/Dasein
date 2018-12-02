#!/usr/bin/env bash

function process_error() {
    if [ ! -z "$(cat /tmp/errors.txt)" ]
    then
        echo "Error!"
        cat /tmp/errors.txt
        exit -1
    fi
}

function build_docker() {
	IMAGE=$1
    NO_CACHE=$2
    echo "Building docker image ${IMAGE} in ${PWD}"
    if [ -f "Dockerfile.tmp" ]; then
        rm Dockerfile.tmp
    fi
	sed "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile> Dockerfile.tmp
	if [ "${NO_CACHE}" == "--no-cache" ]; then
	    docker build --no-cache -f Dockerfile.tmp -t ${IMAGE} . || true
	else
	    docker build -f Dockerfile.tmp -t ${IMAGE} . || true
	fi
	rm Dockerfile.tmp
}

function create_network() {

    if [ -z "$1" ]; then
        echo please specify network name
        exit -1
    fi

    NETWORK=$1

    if [ -z "$(docker network ls -f name=${NETWORK} | grep ${NETWORK})" ]; then
        echo "creating network ${NETWORK} ..."
        docker network create ${NETWORK}
    else
        echo "network ${NETWORK} already exists"
    fi

}

function verify_consul() {
    if [ -z "$(docker ps | grep consul)" ]; then
        echo "must start the consul docker first!"
        docker_ps
        exit -1
    fi
}

function rm_by_label() {

    if [ "$#" -lt 2 ]; then
      echo "Usage: $0 LABEL VALUE" >&2
      exit 1
    fi

    LABEL=$1
    VALUE=$2

    for container in $(docker ps --format "table {{.Names}}" --filter=label=${LABEL}=${VALUE});
    do
        if [ $container == "NAMES" ]; then continue ; fi && \
        echo stopping $container ... && \
        docker stop $container &
    done

    wait

    docker rm $(docker ps -a -q) 2> /dev/null
    docker rmi -f $(docker images -a --filter "dangling=true" -q --no-trunc) 2> /dev/null

    docker_ps
}

function register_service() {

    if [ "$#" -lt 3 ]; then
      echo "Usage: $0 SERVICE CONTAINER PORT" >&2
      exit 1
    fi

    SERVICE=$1
    CONTAINER=$2
    PORT=$3

    IP=$(docker inspect --format="{{.NetworkSettings.Networks.lenet.IPAddress}}" ${CONTAINER})
    HOST=$(docker inspect --format="{{.Config.Hostname}}" ${CONTAINER})

    curl -X POST -d \
    "{\"Node\":\"${HOST}\",\"Address\":\"${IP}\",\"Service\":{\"ServiceID\":\"${SERVICE}\",\"Service\":\"${SERVICE}\",\"Port\":${PORT}}}" \
    http://localhost:8500/v1/catalog/register
}

function deregister_service() {
    if [ "$#" -lt 2 ]; then
      echo "Usage: $0 SERVICE CONTAINER" >&2
      exit 1
    fi

    SERVICE=$1
    CONTAINER=$2
    HOST=$(docker inspect --format="{{.Config.Hostname}}" ${CONTAINER})

    if [ ! -z ${HOST} ]; then
        curl -X PUT -d "{\"Node\":\"${HOST}\",\"ServiceID\":\"${SERVICE}\"}" \
        http://localhost:8500/v1/catalog/deregister
    fi
}

function show_service() {
    if [ "$#" -lt 1 ]; then
      echo "Usage: $0 SERVICE" >&2
      exit 1
    fi

    SERVICE=$1

    curl -X GET http://localhost:8500/v1/catalog/service/${SERVICE}
    echo ""
}

function deregister_by_label() {

    if [ "$#" -lt 3 ]; then
      echo "Usage: $0 LABEL VALUE SERVICE" >&2
      exit 1
    fi

    LABEL=$1
    VALUE=$2
    SERVICE=$3

    for container in $(docker ps --format "table {{.Names}}" --filter=label=${LABEL}=${VALUE});
    do
        if [ $container == "NAMES" ]; then continue ; fi
        echo dergistering service ${SERVICE} on ${container}
        deregister_service ${SERVICE} ${container}
    done

    show_service ${SERVICE}
}

function teardown_simple_service() {

    if [ "$#" -lt 1 ]; then
      echo "Usage: $0 SERVICE CLUSTER" >&2
      exit 1
    fi

    SERVICE=$1
    CLUSTER=$2
    CLUSTER="${CLUSTER:=${SERVICE}}"

    echo "tearing down ${SERVICE} cluster named ${CLUSTER} ..."

    deregister_by_label "${SERVICE}.group" ${CLUSTER} ${SERVICE}
    rm_by_label "${SERVICE}.group" ${CLUSTER}

    return 0
}

function docker_ps() {
    docker ps --format "table {{.Names}}\t{{.Image}}"
}
