#!/usr/bin/env bash

function process_error() {
    IMAGE=$1 &&

    if [[ ! -z "$(cat /tmp/${IMAGE}-errors.txt)" ]]
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

    UNAME=`uname`

    DIR="${PWD}"
	rm -rf ${WORKSPACE}
	mkdir -p ${WORKSPACE}/webapps/${TGT_WAR}
	cd ${WORKSPACE}/webapps/${TGT_WAR}
	jar xvf ${DIR}/webapps/${SRC_WAR}.war
	# replace log4j.properties
	if [[ "${SRC_WAR}" = "scoringapi" ]]; then
	    cp -f ${DIR}/log4j2-scoringapi.xml WEB-INF/classes/log4j2.xml
	else
	    cp -f ${DIR}/log4j2.xml WEB-INF/classes/log4j2.xml
	fi
	if [[ "${UNAME}" == 'Darwin' ]]; then
        sed -i '' "s|{{APP}}|${SRC_WAR}|g" WEB-INF/classes/log4j2.xml
    else
        sed -i "s|{{APP}}|${SRC_WAR}|g" WEB-INF/classes/log4j2.xml
    fi

	# add context.xml
	cp -f ${DIR}/context.xml META-INF/context.xml
	# replace web.xml
	line=$(grep -n 'description' WEB-INF/web.xml | cut -d ":" -f 1)
    { head -n ${line} WEB-INF/web.xml; cat ${DIR}/tomcat_filters.xml; tail -n +$(($line+1)) WEB-INF/web.xml; } > WEB-INF/web2.xml
    mv -f WEB-INF/web2.xml WEB-INF/web.xml
	cd ..
	if [[ -f "${TGT_WAR}/META-INF/MANIFEST.MF" ]]; then
	    echo "found MANIFEST.MF"
	    cat ${TGT_WAR}/META-INF/MANIFEST.MF
	    jar cvmf ${TGT_WAR}/META-INF/MANIFEST.MF ${TGT_WAR}.war -C ${TGT_WAR}/ .
	else
	    jar cvf ${TGT_WAR}.war -C ${TGT_WAR}/ .
	fi
	rm -rf ${TGT_WAR}

	cd ${DIR}
	if [[ "${SRC_WAR}" == "saml" ]] || [[ "${SRC_WAR}" == "pls" ]] || [[ "${SRC_WAR}" == "eai" ]]; then
	    cp ${DIR}/Dockerfile_j8 ${WORKSPACE}/Dockerfile
	else
	    cp ${DIR}/Dockerfile ${WORKSPACE}/Dockerfile
	fi

	pushd ${WORKSPACE}
	if [[ "${UNAME}" == 'Darwin' ]]; then
	    sed -i '' "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
        sed -i '' "s|{{WAR}}|${TGT_WAR}|g" Dockerfile
    else
	    sed -i "s|{{TIMESTAMP}}|$(date +%s)|g" Dockerfile
        sed -i "s|{{WAR}}|${TGT_WAR}|g" Dockerfile
    fi
    docker build -t ${IMAGE} . 2>/tmp/${IMAGE}-errors.txt
    process_error ${IMAGE}
    popd
}

MICROSERVICES=$1

if [[ "${MICROSERVICES}" = "" ]]; then
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
    MICROSERVICES="${MICROSERVICES},datacloudapi"
    MICROSERVICES="${MICROSERVICES},propdata"
    MICROSERVICES="${MICROSERVICES},dellebi"
    MICROSERVICES="${MICROSERVICES},objectapi"
    MICROSERVICES="${MICROSERVICES},dante"
    MICROSERVICES="${MICROSERVICES},saml"
    MICROSERVICES="${MICROSERVICES},playmaker"
fi

mkdir -p /tmp/latticeengines || true
mkdir tmp || true

for service in $(echo $MICROSERVICES | sed "s/,/ /g"); do
    WAR=${service} &&
    if [[ "${WAR}" = "api" ]] || [[ "${WAR}" = "admin" ]] || [[ "${WAR}" = "pls" ]] || [[ "${WAR}" = "matchapi" ]] || [[ "${WAR}" = "scoringapi" ]] || [[ "${WAR}" = "ulysses" ]] || [[ "${WAR}" = "saml" ]] || [[ "${WAR}" = "playmaker" ]]; then
        WAR=ROOT
    fi &&
    IMAGE=latticeengines/${service} &&
    if [[ "${service}" != "oauth2" ]]; then
        build_docker ${IMAGE} ${service} ${WAR}
    fi
done

rm -rf tmp
rm -rf /tmp/latticeengines
