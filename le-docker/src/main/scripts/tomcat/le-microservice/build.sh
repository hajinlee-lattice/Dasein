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
	SRC_WAR=$2
	TGT_WAR=$3
	WORKSPACE=tmp/${SRC_WAR}

	rm -rf ${WORKSPACE}
	mkdir -p ${WORKSPACE}/webapps
	cp webapps/${SRC_WAR}.war ${WORKSPACE}/webapps/${TGT_WAR}.war
	cp Dockerfile ${WORKSPACE}

	pushd ${WORKSPACE}
    sed -i "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
    sed -i "s|{{WAR}}|${TGT_WAR}|g" Dockerfile
    docker build -t ${IMAGE} . 2>/tmp/${IMAGE}-errors.txt
    process_error ${IMAGE}
    popd
}

MICROSERVICES=$1

if [ "${MICROSERVICES}" = "" ]; then
    MICROSERVICES="pls"
    MICROSERVICES="${MICROSERVICES},admin"
    MICROSERVICES="${MICROSERVICES},matchapi"
    MICROSERVICES="${MICROSERVICES},scoringapi"
    MICROSERVICES="${MICROSERVICES},ulysses"
    MICROSERVICES="${MICROSERVICES},eai"
    MICROSERVICES="${MICROSERVICES},metadata"
    MICROSERVICES="${MICROSERVICES},scoring"
    MICROSERVICES="${MICROSERVICES},modeling"
    MICROSERVICES="${MICROSERVICES},dataflowapi"
    MICROSERVICES="${MICROSERVICES},workflowapi"
    MICROSERVICES="${MICROSERVICES},quartz"
    MICROSERVICES="${MICROSERVICES},modelquality"
    MICROSERVICES="${MICROSERVICES},propdata"
    MICROSERVICES="${MICROSERVICES},dellebi"
fi

mkdir -p /tmp/latticeengines || true
mkdir tmp || true

for service in $(echo $MICROSERVICES | sed "s/,/ /g"); do
    WAR=${service} &&
    if [ "${WAR}" = "admin" ] || [ "${WAR}" = "pls" ] || [ "${WAR}" = "matchapi" ] || [ "${WAR}" = "scoringapi" ] || [ "${WAR}" = "ulysses" ]; then
        WAR=ROOT
    fi &&
    IMAGE=latticeengines/${service} &&
    build_docker ${IMAGE} ${service} ${WAR} &
done
wait

rm -rf tmp
rm -rf /tmp/latticeengines