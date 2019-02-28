#!/usr/bin/env bash

function process_error() {
    if [[ ! -z "$(cat /tmp/errors.txt)" ]]
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
    if [[ -f "Dockerfile.tmp" ]]; then
        rm Dockerfile.tmp
    fi
	sed "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile> Dockerfile.tmp
	if [[ "${NO_CACHE}" == "--no-cache" ]]; then
	    docker build --no-cache -f Dockerfile.tmp -t ${IMAGE} . || true
	else
	    docker build -f Dockerfile.tmp -t ${IMAGE} . || true
	fi
	rm Dockerfile.tmp
}

function build_docker2() {
	IMAGE=$1
    NO_CACHE=$2
    echo "Building docker image ${IMAGE} in ${PWD}"
    if [[ -f "Dockerfile.tmp" ]]; then
        rm Dockerfile.tmp
    fi
	sed "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile2> Dockerfile2.tmp
	if [[ "${NO_CACHE}" == "--no-cache" ]]; then
	    docker build --no-cache -f Dockerfile2.tmp -t ${IMAGE} . || true
	else
	    docker build -f Dockerfile2.tmp -t ${IMAGE} . || true
	fi
	rm Dockerfile2.tmp
}

function docker_ps() {
    docker ps --format "table {{.Names}}\t{{.Image}}"
}
