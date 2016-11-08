#!/usr/bin/env bash

function process_error() {
    IMAGE=$1 &&

    if [ ! -z "$(cat /tmp/${IMAGE}-errors.txt)" ]
    then
        echo "Error!";
        cat /tmp/${IMAGE}-errors.txt;
        exit -1
    fi
}

function build_docker() {
	IMAGE=$1
	WAR=$2
	WORKSPACE=tmp/${WAR}

	rm -rf ${WORKSPACE}
	mkdir -p ${WORKSPACE}/webapps
	cp -r META-INF ${WORKSPACE}
	cp webapps/${WAR}.war ${WORKSPACE}/webapps/${WAR}.war
	cp Dockerfile ${WORKSPACE}

	pushd ${WORKSPACE}
    sed -i "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
    sed -i "s|{{WAR}}|${WAR}|g" Dockerfile
    docker build -t $IMAGE . 2>/tmp/${IMAGE}-errors.txt
    process_error ${IMAGE}
    popd
}

MICROSERVICES="doc"
MICROSERVICES="${MICROSERVICES}\n eai"
MICROSERVICES="${MICROSERVICES}\n metadata"
MICROSERVICES="${MICROSERVICES}\n scoring"
MICROSERVICES="${MICROSERVICES}\n modeling"
MICROSERVICES="${MICROSERVICES}\n dataflowapi"
MICROSERVICES="${MICROSERVICES}\n workflowapi"
MICROSERVICES="${MICROSERVICES}\n quartz"
MICROSERVICES="${MICROSERVICES}\n modelquality"
MICROSERVICES="${MICROSERVICES}\n propdata"
MICROSERVICES="${MICROSERVICES}\n dellebi"

mkdir -p /tmp/latticeengines || true
mkdir tmp || true

for service in ${MICROSERVICES}; do
    WAR=${service} &&
    if [ "${WAR}" = "doc" ]; then
        WAR=ROOT
    fi &&
    IMAGE=latticeengines/${service} &&
    build_docker $IMAGE $WAR &
done
wait

rm -rf tmp
rm -rf /tmp/latticeengines